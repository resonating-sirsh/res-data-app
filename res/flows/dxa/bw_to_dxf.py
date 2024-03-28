from res.flows.dxa import node_run

import asyncio


async def _start_dxf_export(bw_s3_path) -> str:
    bundle_job = await node_run.submit(
        run_type="DXA_EXPORT_ASSET_BUNDLE",
        preset_key="dxf_v0",
        details={"input_file_uri": bw_s3_path},
    )
    return bundle_job[0].get("id")


async def _get_job_dxf(job_id):
    # We could use subscriptions here. For ease, we'll just poll for now.
    while True:
        dxf_job = await node_run.fetch_by_id(job_id)
        dxf_zip = [
            x for x in dxf_job.get("assets") if x.get("name") == "dxf_bundle.zip"
        ]
        if len(dxf_zip) > 0:
            return dxf_zip[0].get("path")
        else:
            await asyncio.sleep(10)
            print("no dxf yet. I'm a little noisy, so consider removing me.")


async def run(bw_s3_path) -> str:
    """
    Convert a .bw file to a dxf.

    Returns: S3 path for the zipped asset bundle containing the dxf.

    usage:
    - if an event loop is already running (as in a Jupyter Notebook):
      - `await run(bw_s3_path)`
    - else:
      - asyncio.run(run(bw_s3_path))
    """
    job_id = await _start_dxf_export(bw_s3_path)
    return await _get_job_dxf(job_id)
