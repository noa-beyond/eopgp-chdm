from __future__ import annotations

import os
import re
import logging
import random
import string
import pathlib
import datetime

import requests
import requests_aws4auth
from concurrent.futures import ThreadPoolExecutor, as_completed

from collections import defaultdict

import numpy as np
import xarray as xr
import rioxarray
import rasterio
from rasterio.windows import from_bounds

import geopandas as gpd
from shapely.geometry import shape, box

import torch
from torch.utils.data import Dataset

from kafka.errors import NoBrokersAvailable

from noachdm.models.BIT import define_G

from noachdm.messaging.kafka_producer import KafkaProducer
from noachdm.messaging.message import Message

from typing import Tuple, Optional

from numcodecs import Blosc


def send_kafka_message(bootstrap_servers, topic, result, order_id, product_path):
    logger = logging.getLogger(__name__)
    schema_def = Message.schema_response()

    try:
        producer = KafkaProducer(bootstrap_servers=bootstrap_servers, schema=schema_def)
        kafka_message = {
            "result": result,
            "orderId": order_id,
            "chdmProductPath": product_path,
        }
        producer.send(topic=topic, key=None, value=kafka_message)
        logger.debug("Kafka message of New ChDM Product sent to: %s", topic)
    except NoBrokersAvailable as e:
        logger.warning("No brokers available. Continuing without Kafka. Error: %s", e)
    except BrokenPipeError as e:
        logger.error("Error sending kafka message to topic %s: %s ", topic, e)


def get_bbox(geometry: dict) -> tuple[float, float, float, float]:
    """
    Extract a bbox from a Geojson Geometry

    Parameters:
        geometry (dict): Geojson Geometry
    """
    return shape(geometry).bounds


def crop_and_make_mosaic(
    items_paths: list[str],
    bbox: tuple[float, float, float, float],
    output_path: pathlib.Path,
) -> pathlib.Path:
    """
    There is a lower (hardcoded for now) limit on kernel for images.
    Even though we say crop, if the bbox is smaller than this lower limit,
    we apply the lower limit instead.
    Moreover, this function crops and then combines the images in a mosaic,
    by applying a median calculation.
    This is true in either adjacent tiles case, or in multidate extent cases,
    where the exact requested date was not found

    Parameters:
        items_paths (list[str|Path]): the list of paths to be cropped and merged
        bbox (tuple[float, float, float, float]): Bbox to be cropped against
        output_path (pathlib.Path): Output directory to store mosaics

    Returns:
        output_filename (str): Output mosaic filename
    """

    bands = ["B02", "B03", "B04"]
    for band in bands:
        a_filename = None
        cropped_list = []
        for path in items_paths:
            granule_path = pathlib.Path(path, "GRANULE")
            dirs = [d for d in granule_path.iterdir() if d.is_dir]
            resolved_path = pathlib.Path(granule_path, dirs[0], "IMG_DATA", "R10m")
            for raster in os.listdir(resolved_path):
                raster_path = pathlib.Path(resolved_path, raster)
                if band in raster:
                    if a_filename is None:
                        a_filename = pathlib.Path(raster_path).name
                    da = rioxarray.open_rasterio(raster_path)
                    gdf = gpd.GeoDataFrame(geometry=[box(*bbox)], crs="EPSG:4326")
                    gdf_proj = gdf.to_crs(da.rio.crs)
                    da_clipped = da.rio.clip(gdf_proj.geometry, gdf_proj.crs)
                    cropped_list.append(da_clipped)
                    continue
        # If more than one path (bbox exceeds one tile or multiple dates)
        if len(cropped_list) > 1:
            reference = cropped_list[0]
            reprojected = [da.rio.reproject_match(reference) for da in cropped_list]
            stacked = xr.concat(reprojected, dim="stack")
            result = stacked.median(dim="stack")
        elif len(cropped_list) == 1:
            result = cropped_list[0]
        else:
            raise RuntimeError("Invalid input items. Are you using Sentinel2 L2A?")

        # Ensure result has spatial reference info
        result.rio.write_crs(result.rio.crs, inplace=True)

        # TODO add a more meaningful file name maybe
        tile_pattern = r"T\d{2}[A-Z]{3}"
        date_pattern = r"\d{4}(0[1-9]|1[0-2])(0[1-9]|[12][0-9]|3[01])"

        tile_match = re.search(tile_pattern, a_filename)
        date_match = re.search(date_pattern, a_filename)

        # TODO still needs a better naming, but be careful to have the band at the end
        # or change code below at "group_bands" function, at line
        # `scene_id = "_".join(fname.name.split("_")[:-1])` to get the scene id.
        filename = "_".join([tile_match.group(), date_match.group(), "composite", band])
        output_path.mkdir(parents=True, exist_ok=True)
        output_filename = pathlib.Path(output_path, filename + ".tif")
        result.rio.to_raster(
            output_filename,
            driver="GTiff",
        )

    return output_filename


class SentinelChangeDataset(Dataset):
    """
    Dataset Class all data needed for prediction: pre and post scenes,
    grouped bands. Implements torch->Dataset
    """

    def __init__(self, pre_dir, post_dir):
        self.pre_scenes = self._group_bands(pre_dir)
        self.post_scenes = self._group_bands(post_dir)

        # Dynamically compute patch sizes and strides per scene
        self.patch_coords = []
        for idx, scene in enumerate(self.pre_scenes):
            print(f"idx: {idx} SCENE: {self.pre_scenes[idx]}")
            with rasterio.open(scene["B04"]) as src:
                h, w = src.height, src.width
                min_dim = min(h, w)
                patch_size = max(128, min((3 * min_dim) // 4, 2048))

                self.patch_size = _closest_even(patch_size)
                self.stride = (3 * self.patch_size) // 4
                self.stride = _closest_even(self.stride)

                for y in range(0, h, self.stride):
                    for x in range(0, w, self.stride):
                        if y + self.patch_size > h:
                            y = h - self.patch_size
                        if x + self.patch_size > w:
                            x = w - self.patch_size
                        self.patch_coords.append((idx, y, x))

    def _group_bands(self, folder: pathlib.Path):
        grouped = defaultdict(dict)
        for fname in folder.iterdir():
            if fname.is_file():
                for band in ["B04", "B03", "B02"]:
                    if band in fname.name:
                        scene_id = "_".join(fname.name.split("_")[:-1])
                        grouped[scene_id][band] = str(pathlib.Path(folder, fname))
        return list(grouped.values())

    def __len__(self):
        return len(self.patch_coords)

    def __getitem__(self, idx):
        scene_id, y, x = self.patch_coords[idx]

        def read_patch(band_paths):
            patch = []
            for b in ["B04", "B03", "B02"]:
                with rasterio.open(band_paths[b]) as src:
                    window = rasterio.windows.Window(
                        x, y, self.patch_size, self.patch_size
                    )
                    patch.append(src.read(1, window=window))
            patch = np.stack(patch)
            patch = np.clip(patch / 15000.0, 0, 1)
            return torch.tensor(patch, dtype=torch.float32)

        pre_patch = read_patch(self.pre_scenes[scene_id])
        post_patch = read_patch(self.post_scenes[scene_id])

        return pre_patch, post_patch


def predict_all_scenes_to_mosaic(
    model_weights_path: str,
    dataset: SentinelChangeDataset,
    output_dir: pathlib.Path,
    device="cpu",
    logger=logging.getLogger(__name__),
):
    """
    Performs the prediction and extracts the outputs

    Parameters:
        model_weights_path (str): Which ML model to use,
        dataset (SentinelChangeDataset): Dataset instance of pre/post change rasters,
        output_dir (pathlib.Path): output dir,
        device (str): which device to use for inference. Currently only "cpu",
        logger (logging.logger): logger instance,
    Returns:
        (filename)_pred.tif: Binary Change Prediction raster (0/1)
        (filename)_pred_logits.tif: Confidence Prediction raster (normalized)
        (filename).zarr: Zarr folder of both prediction files
    """
    model = define_G(net_G="base_transformer_pos_s4_dd8", input_nc=3)
    model = torch.load(
        model_weights_path, weights_only=False, map_location=torch.device(device)
    )
    model.eval()
    model.to(device)

    tile_pattern = r"T\d{2}[A-Z]{3}"
    date_pattern = r"\d{4}(0[1-9]|1[0-2])(0[1-9]|[12][0-9]|3[01])"

    tile_match = re.search(
        tile_pattern, pathlib.Path(dataset.pre_scenes[0]["B04"]).name
    )
    date_match_from = re.search(
        date_pattern, pathlib.Path(dataset.pre_scenes[0]["B04"]).name
    )
    date_match_to = re.search(
        date_pattern, pathlib.Path(dataset.post_scenes[0]["B04"]).name
    )

    date_from = date_match_from.group()
    date_to = date_match_to.group()
    tile = tile_match.group()
    random_choice = random.choices(string.ascii_letters + string.digits, k=6)
    logger.info(
        "Filename parts: %s, %s, %s, %s", date_from, date_to, tile, random_choice
    )

    for scene_index, scene in enumerate(dataset.pre_scenes):
        with rasterio.open(scene["B04"]) as ref_src:
            h, w = ref_src.height, ref_src.width
            transform = ref_src.transform
            crs = ref_src.crs

        full_pred = np.zeros((h, w), dtype=np.uint8)
        full_pred_logits = np.zeros((h, w), dtype=np.uint8)

        # Predict patches for this scene only
        for idx, (scene_id, y, x) in enumerate(dataset.patch_coords):
            if scene_id != scene_index:
                continue

            pre_tensor, post_tensor = dataset[idx]
            pre_tensor = pre_tensor.unsqueeze(0).to(device)
            post_tensor = post_tensor.unsqueeze(0).to(device)

            with torch.no_grad():
                output = model(pre_tensor, post_tensor)
                # pred_patch =
                #   torch.argmax(output, dim=1).cpu().numpy().astype(np.uint8)
                pred_patch_logits = (
                    torch.softmax(output, dim=1).detach().cpu().numpy()[0, 1, :, :]
                    * 100
                ).astype(np.uint8)

            y_shift = pred_patch_logits.shape[0]
            x_shift = pred_patch_logits.shape[1]
            y = min(y, h - y_shift)
            x = min(x, w - x_shift)
            full_pred_logits[y : y + y_shift, x : x + x_shift] = pred_patch_logits

        full_pred_logits = full_pred_logits.astype(np.float32)

        # Standardize
        mean_val = np.mean(full_pred_logits)
        std_val = np.std(full_pred_logits)
        std_logits = (full_pred_logits - mean_val) / (std_val + 1e-8)

        # Scale to 0–100
        full_pred_logits = (std_logits * 10 + 50).clip(0, 100).astype(np.uint8)

        min_val = full_pred_logits.min()
        max_val = full_pred_logits.max()
        full_pred_logits = (
            (full_pred_logits - min_val) / (max_val - min_val) * 100
        ).astype(np.uint8)

        full_pred[full_pred_logits >= 50] = 1
        full_pred = full_pred.astype(np.uint8)

        # Save individual scene prediction. E.g.
        # ChDM_S2_20220215_20230316_TJ35_AD6548_pred.tif
        filename_parts = ["ChDM_S2", date_from, date_to, tile, "".join(random_choice)]
        output_filename = "_".join(filename_parts)
        output_filename_pred = output_filename + "_pred.tif"
        output_filename_pred_logits = output_filename + "_pred_logits.tif"
        output_dir.resolve().mkdir(parents=True, exist_ok=True)
        output_path_pred = pathlib.Path(output_dir.resolve(), output_filename_pred)
        output_path_logits = pathlib.Path(
            output_dir.resolve(), output_filename_pred_logits
        )

        _write_cog(
            path=output_path_pred,
            array=full_pred,
            height=h,
            width=w,
            dtype=full_pred.dtype,
            crs=crs,
            transform=transform,
            nodata=None,
            compress="DEFLATE",
            blocksize=512,
            bigtiff="IF_SAFER",
            overview_resampling="NEAREST",
            num_threads="ALL_CPUS",
        )

        _write_cog(
            path=output_path_logits,
            array=full_pred_logits,
            height=h,
            width=w,
            dtype=full_pred_logits.dtype,
            crs=crs,
            transform=transform,
            nodata=None,
            compress="DEFLATE",
            blocksize=512,
            bigtiff="IF_SAFER",
            overview_resampling="NEAREST",
            num_threads="ALL_CPUS",
        )

        logger.info(
            f"Successfully created COGs: {output_path_pred}, {output_path_logits}"
        )

        out_zarr = output_dir.resolve() / f"{output_filename}.zarr"

        try:
            zarr_path = stack_geotiffs_to_zarr(
                pred_path=output_path_pred,
                logits_path=output_path_logits,
                out_zarr=out_zarr,
                chunks=(1024, 1024),
                add_time_dim=True,
                time_coord="start",
            )
            logger.info("Zarr stacked at %s", str(zarr_path))

        except Exception as e:
            logger.exception(
                "Failed to build Zarr for %s and %s: %s",
                output_path_pred.name,
                output_path_logits.name,
                e,
            )

        s3_upload_path = _upload_to_s3(output_path_pred, output_path_logits, zarr_path)
        return s3_upload_path


def _upload_to_s3(
    output_path_pred: pathlib.Path,
    output_path_logits: pathlib.Path,
    zarr_path: pathlib.Path,
):
    logger = logging.getLogger(__name__)
    region = os.getenv("CREODIAS_REGION", None)
    service = "s3"
    endpoint = os.getenv("CREODIAS_ENDPOINT", None)
    bucket_name = os.getenv("CREODIAS_S3_BUCKET_PRODUCT_OUTPUT")
    auth = requests_aws4auth.AWS4Auth(
        os.getenv("CREODIAS_S3_ACCESS_KEY"),
        os.getenv("CREODIAS_S3_SECRET_KEY"),
        region,
        service,
    )
    bucket_name = bucket_name + "/products"

    current_date = datetime.datetime.now().strftime("%Y%m%d")
    random_choice = random.choices(string.ascii_letters + string.digits, k=6)
    current_date_plus_random = current_date + "_" + "".join(random_choice)

    for product_path in [output_path_pred, output_path_logits]:
        with open(product_path, "rb") as file_data:
            file_content = file_data.read()
        headers = {
            "Content-Length": str(len(file_content)),
        }
        url = f"{endpoint}/{bucket_name}/{current_date_plus_random}/{str(product_path.name)}"

        try:
            response = requests.put(
                url, data=file_content, headers=headers, auth=auth, timeout=300
            )
        except requests.exceptions.Timeout:
            logger.error("Timeout when uploading %s", product_path)

        if response.status_code == 200:
            logger.info(
                "Successfully uploaded %s to %s, %s bucket in %s folder",
                str(product_path.name),
                endpoint,
                bucket_name,
                current_date_plus_random,
            )
        else:
            logger.error(
                "Could not upload %s to %s: %s",
                str(product_path.name),
                bucket_name,
                response.text,
            )
    upload_zarr_folder(zarr_path, auth, endpoint, bucket_name, current_date_plus_random)

    return f"{endpoint}/{bucket_name}/{current_date_plus_random}/"


def crop_to_reference(reference_path: pathlib.Path, raster_path: pathlib.Path):
    """
    Crop a raster in place against a reference

    Parameters:
        reference_path (pathlib.Path): Reference,
        raster_path (pathlib.Path): Target

    Returns:
        Cropped raster in raster_path
    """
    with rasterio.open(reference_path) as ref:
        ref_bounds = ref.bounds
        ref_transform = ref.transform
        ref_nodata = ref.nodata if ref.nodata is not None else 0
        ref_profile = ref.profile.copy()
        ref_profile.update({"nodata": ref_nodata})

    with rasterio.open(raster_path) as src:
        if src.bounds == ref_bounds and src.transform == ref_transform:
            return False

        window = from_bounds(*ref_bounds, transform=src.transform)
        window = window.round_offsets().round_lengths()
        try:
            data = src.read(window=window, boundless=True, fill_value=ref_nodata)
        except Exception as e:
            raise RuntimeError(f"Error reading {raster_path.name}") from e

        transform = src.window_transform(window)
        profile = src.profile.copy()
        profile.update(
            {
                "height": data.shape[1],
                "width": data.shape[2],
                "transform": transform,
                "nodata": ref_nodata,
            }
        )

    with rasterio.open(raster_path, "w", **profile) as dst:
        dst.write(data)
    return True


def stack_geotiffs_to_zarr(
    pred_path: pathlib.Path,
    logits_path: pathlib.Path,
    out_zarr: pathlib.Path,
    chunks: Tuple[int, int] = (1024, 1024),
    time_from: Optional[str] = None,
    time_to: Optional[str] = None,
    add_time_dim: bool = True,
    time_coord: str = "start",
) -> pathlib.Path:
    """
    Stack a binary mask (pred) and score (logits) GeoTIFFs into a single Zarr store.

    Parameters:
        pred_path (pathlib.Path): Prediction file path,
        logits_path (pathlib.Path): Prediction confidence file path,
        out_zarr (pathlib.Path): Zarr directory path,
        chunks (Tuple[int, int]): zarr chunk size,
        time_from (str): TimeSpan start,
        time_to (str): TimeSpan end,
        add_time_dim (bool): Add the time dimension. Defaults to True,
        time_coord (str): Time coordinate (start, midpoint, end)

    Returns
        zarr_path (pathlib.Path): the zarr directory
    """
    logger = logging.getLogger(__name__)

    logger.info(
        "Stacking GeoTIFFs to Zarr: pred=%s logits=%s -> %s",
        pred_path.name,
        logits_path.name,
        out_zarr.name,
    )
    logger.debug("Chunking configured as (y, x)=%s", chunks)

    crop_to_reference(pred_path, logits_path)

    # Open and squeeze band -> (y, x), while it keeps chunking
    predictions = (
        rioxarray.open_rasterio(pred_path, chunks={"y": chunks[0], "x": chunks[1]})
        .squeeze("band", drop=True)
        .astype("uint8")
    )

    logits = (
        rioxarray.open_rasterio(logits_path, chunks={"y": chunks[0], "x": chunks[1]})
        .squeeze("band", drop=True)
        .astype("uint8")
    )
    logger.debug(
        "Opened rasters: pred shape=%s, logits shape=%s",
        str(predictions.shape),
        str(logits.shape),
    )
    logger.debug(
        "CRS: pred=%s, logits=%s", str(predictions.rio.crs), str(logits.rio.crs)
    )

    # Reproject/align if needed (shouldn't be, after crop)
    if logits.rio.crs != predictions.rio.crs or logits.shape != predictions.shape:
        logger.info("Reprojecting logits to match pred")
        logits = logits.rio.reproject_match(predictions)

    predictions.name = "change"
    logits.name = "score"

    ds = xr.Dataset({"change": predictions, "score": logits})
    ds = ds.assign_coords(x=predictions.x, y=predictions.y)
    ds.rio.write_crs(predictions.rio.crs, inplace=True)

    ds["change"].attrs.update(
        {"long_name": "Binary change mask", "flag_values": [0, 1]}
    )
    ds["score"].attrs.update({"long_name": "Change score (0-100)"})

    # add time metadata
    if time_from is None or time_to is None:
        parsed_time_from, parsed_time_to = _parse_dates_from_standard_name(pred_path)
        time_from = time_from or parsed_time_from
        time_to = time_to or parsed_time_to

    t_start = _date_to_datetime(time_from)
    t_end = _date_to_datetime(time_to)

    ds = ds.assign_coords(
        time_start=xr.DataArray(t_start),
        time_end=xr.DataArray(t_end),
        period=f"{time_from}-{time_to}",
    )

    if add_time_dim:
        if time_coord == "start":
            t = t_start
        elif time_coord == "end":
            t = t_end
        else:
            raise ValueError(
                f"Invalid time_coord='{time_coord}'. Use 'start'|'end'|'midpoint'."
            )

        ds = ds.expand_dims({"time": [t]})
        logger.debug("Added time dim with coord=%s value=%s", time_coord, t)

    # Compression --> chunked Zarr
    compressor = Blosc(cname="zstd", clevel=3, shuffle=Blosc.BITSHUFFLE)
    encoding = {
        "change": {"compressor": compressor, "chunks": (chunks[0], chunks[1])},
        "score": {"compressor": compressor, "chunks": (chunks[0], chunks[1])},
    }

    out_zarr = pathlib.Path(out_zarr)
    logger.info("Writing Zarr -> %s ", (out_zarr))

    ds.to_zarr(out_zarr, mode="w", consolidated=True, encoding=encoding)
    logger.info("Zarr write complete: %s", out_zarr)

    return out_zarr


def upload_zarr_folder(
    local_zarr_path: pathlib.Path,
    auth: requests_aws4auth.AWS4Auth,
    endpoint: str,
    bucket_name: str,
    prefix: str,
    max_workers: int = 8,
):
    """
    Upload a zarr directory to S3

    Parameters:
        local_zarr_path (pathlib.Path): Path to the local .zarr directory
        auth (requests_aws4auth.AWS4Auth): authentication instance
        endpoint (str): Base endpoint URL
        bucket_name (str): Bucket name
        prefix (str): Remote prefix (e.g. current_date_plus_random)
        max_workers (int): Number of parallel uploads
    """
    logger = logging.getLogger(__name__)
    upload_tasks = []
    for root, _, files in os.walk(local_zarr_path):
        for file_name in files:
            local_path = os.path.join(root, file_name)
            rel_path = os.path.relpath(local_path, local_zarr_path)
            url = f"{endpoint}/{bucket_name}/{prefix}/{local_zarr_path.name}/{rel_path}"
            upload_tasks.append((local_path, url))

    # Move .zmetadata to the end of the upload list
    upload_tasks.sort(key=lambda x: x[0].endswith(".zmetadata"))

    logger.info(
        "Uploading %d files from %s to %s/%s/%s/%s.zarr/",
        len(upload_tasks),
        local_zarr_path.name,
        endpoint,
        bucket_name,
        prefix,
        prefix,
    )

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_upload_file, path, url, auth): (path, url)
            for path, url in upload_tasks
        }
        for future in as_completed(futures):
            path, url = futures[future]
            try:
                future.result()
                logger.info("Uploaded: %s", os.path.relpath(path, local_zarr_path))
            except Exception as e:
                logger.info(
                    "Failed: %s - %s", os.path.relpath(path, local_zarr_path), e
                )
    logger.info("Zarr uploaded successfully.")


def _parse_dates_from_standard_name(p: pathlib.Path) -> tuple[str, str]:
    """
    Assumption that the filename will always be in the form of:
    ChDM_S2_<DATE_FROM>_<DATE_TO>_<TILE>_<RAND>_(pred|pred_logits).tif

    Returns (DATE_FROM, DATE_TO) as 'YYYYMMDD' strings.
    """
    stem = p.stem
    parts = stem.split("_")

    assert (
        len(parts) >= 7 and parts[0] == "ChDM"
    ), f"Unexpected filename format: {p.name}"

    return parts[2], parts[3]


def _date_to_datetime(s: str) -> np.datetime64:
    """
    Convert 'YYYYMMDD' to numpy.datetime64 with ns precision.
    """
    return np.datetime64(datetime.datetime.strptime(s, "%Y%m%d"), "ns")


def _closest_even(n):
    if n % 2 == 0:
        return n
    return n - 1 if n % 2 == 1 else n + 1


def _write_cog(
    path: pathlib.Path,
    array: np.ndarray,
    *,
    height: int,
    width: int,
    dtype,
    crs,
    transform,
    nodata=None,
    compress: str = "DEFLATE",
    blocksize: int = 512,
    bigtiff: str = "IF_SAFER",
    overview_resampling: str = "NEAREST",
    num_threads: str = "ALL_CPUS",
    count: int = 1,
):
    profile = {
        "driver": "COG",
        "height": height,
        "width": width,
        "count": count,
        "dtype": dtype,
        "crs": crs,
        "transform": transform,
        "nodata": nodata,
        # COG creation options (GDAL)
        "compress": compress,
        "blocksize": blocksize,
        "bigtiff": bigtiff,
        "num_threads": num_threads,
        "overview_resampling": overview_resampling,
    }

    with rasterio.open(path, "w", **profile) as dst:
        dst.write(array, 1)


def _upload_file(local_path, url, auth):
    with open(local_path, "rb") as f:
        file_data = f.read()
    headers = {"Content-Length": str(len(file_data))}
    response = requests.put(
        url, headers=headers, auth=auth, data=file_data, timeout=360
    )
    response.raise_for_status()
    return url
