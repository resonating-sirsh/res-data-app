from PIL import Image
import res

hasura = res.connectors.load("hasura")
dev_hasura = res.connectors.load("hasura")
s3 = res.connectors.load("s3")

GET_GRAPHAPI_ARTWORKS = """
    query artworkFiles($first: Int! $where: ArtworkFilesWhere $after: String) {
        artworkFiles(first: $first where: $where after: $after) {
            artworkFiles {
                id
                name
                file{
                url
                    s3{
                    key
                    bucket
                }
            }
            }
            hasMore
            cursor
        }
    }
"""

UPSERT_META_ARTWORK = """
    mutation InsertArtwork($object: meta_artwork_insert_input!) {
        insert_meta_artwork_one(
            on_conflict: {constraint: artwork_pkey, update_columns: [updated_at]}, 
            object: $object
        ) {
            id
            name
        }
    }
"""

LATEST_ARTWORK = """
    query latestArtwork {
        meta_artwork(order_by: {created_at: desc}, limit: 1) {
            id
            properties
        }
    }
"""


def save_thumbnail(id, img):
    # create a thumbnail
    aspect = img.size[0] / img.size[1]
    thumb_size = (int(256 * aspect), 256)
    thumb = img.resize(thumb_size, Image.ANTIALIAS)

    # save the thumbnail to s3
    thumb_uri = f"s3://res-temp-public-bucket/style_assets_dev/__test_image_shortner/{id}_thumb.png"
    res.utils.logger.info(f"Saving thumbnail to {thumb_uri}")
    s3.write(thumb_uri, thumb)

    return thumb_uri


def save_reduced(id, img, dpi_x, dpi_y):
    scale = 75 / max(dpi_x, dpi_y)
    [w, h] = [int(img.size[0] * scale), int(img.size[1] * scale)]
    img_75 = img.resize((w, h), Image.ANTIALIAS)

    img_75_uri = f"s3://res-temp-public-bucket/style_assets_dev/__test_image_shortner/{id}_75dpi.png"
    res.utils.logger.info(f"Saving 75dpi version to {img_75_uri}")
    s3.write(img_75_uri, img_75)

    return img_75_uri


def handle_event(event):
    # after is cursor based not id based -- not sure what to do here TODO
    # after = None
    # try:
    #     latest = hasura.execute(LATEST_ARTWORK)
    #     after = latest["meta_artwork"][0]["properties"]["id"]
    # except:
    #     res.utils.logger.info("No latest artwork found")

    # for starters, get a few artworks, downsample and save to hasura
    result = hasura.execute_with_kwargs(GET_GRAPHAPI_ARTWORKS, first=10)
    for artwork in result["artworkFiles"]["artworkFiles"]:
        try:
            id = artwork.get("id")
            file = artwork.get("file")

            if file is None:
                res.utils.logger.info(f"Artwork {id} has no file")
                continue

            # download the file from s3
            uri = f"s3://{file['s3']['bucket']}/{file['s3']['key']}"
            res.utils.logger.info(f"Downloading file {uri} for artwork {id}")
            img = s3.read_image(uri)

            # get dpi assuming 300 if it's not there
            dpi = img.info.get("dpi", [300, 300])
            dpi_x = int(dpi[0])
            dpi_y = int(dpi[1])

            res.utils.logger.info(f"Artwork {id} has dpi {dpi_x}x{dpi_y}")

            thumb_uri = save_thumbnail(id, img)
            img_75_uri = save_reduced(id, img, dpi_x, dpi_y)

            # create a 75dpi version
            id = res.utils.uuid_str_from_dict({"id": id})  # TODO: something better

            result = hasura.execute_with_kwargs(
                UPSERT_META_ARTWORK,
                object={
                    "id": id,
                    "name": artwork.get("name", id),
                    "original": uri,
                    "thumb": thumb_uri,
                    "reduced": img_75_uri,
                    "properties": artwork,
                },
            )
            res.utils.logger.info(f"Upserted artwork {id}")
        except Exception as e:
            res.utils.logger.error(f"Error handling artwork {id}: {e}")


if __name__ == "__main__":
    res.utils.logger.info("Starting image shortner")
    handle_event(None)
