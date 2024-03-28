# start refactoring marker into the cleaner meta one folder
import res


class MetaONEContractException(Exception):
    pass


class StyleNotRegisteredContractException(MetaONEContractException):
    pass


def refresh_body_stampers(event, context={}):
    from res.flows.meta.ONE.meta_one import BodyMetaOne

    failed_assets = []
    with res.flows.FlowContext(event, context) as fc:
        for asset in fc.assets:
            body_code = asset["body_code"]
            body_version = asset["body_version"]
            # here the refresh trust what is there already as source of truth on what we should have
            path = f"s3://meta-one-assets-prod/bodies/cut/{body_code.lower().replace('-','_')}/v{body_version}"
            files_existing = list(res.connectors.load("s3").ls(path))
            if len([s for s in files_existing if "plt" in s]):
                for f in files_existing:
                    if len(f.split("/")) < 8:
                        continue  # invalid path
                    size_code = f.split("/")[7].upper()
                    try:
                        m = BodyMetaOne(body_code, body_version, size_code)
                        m.save_complementary_pieces()
                    except Exception as ex:
                        res.utils.logger.info(
                            f"Failed to load the body, you may want to sync the body data {asset}"
                        )
                        failed_assets.append(asset)
            else:
                res.utils.logger.info(f"Nothing at {path}")
    return {"failed": failed_assets}
