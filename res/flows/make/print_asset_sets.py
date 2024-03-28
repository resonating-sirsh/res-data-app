"""
 NESTING MODULE:
 Uses standard res-flow-node semantics:
 *generator*: performs a plan for nesting and can result in N "pages" that are created in parallel in handler
 *handler*: generates the page of the printfile with composited images and labels (res-color)
 *reducer*: joins the pages together into a single print file as required
 --------------------------------------------------------------------------------------------------------
 It is possible to only call the generator and get the full metadata for the nesting e.g. nested area
 If optimus wants to make print assets only, it can invoke `generator` and supply a nesting result callback
 The module can be run in "plan" mode taking advantage of how all res-flow-nodes work...

     A] To not run the entire module i.e. to plan only, you can call the res-flow-node with
        metadata:{
            name: make.print_asset_sets.generator
        }
        and only this generator function will be run. 
        The data are saved on S3 and a callback can be used to get the nesting result

    B] To run an entire nesting (and make printfile), instead call the module without the generator suffix
        metadata:{
            name: make.print_asset_sets
        }
        and the full generate-map-reduce will run

In all cases the modules payload is as described below - see flow schema in FlowEventProcessor for validator

 --------------------------------------------------------------------------------------------------------
    [metadata]:
    {
        name: make.print_asset_sets.[optional_op]
        version: v0
    }

    [assets] 
    {
        key : one_number
        body_version: the body version
        body_code: resonance_body
        piece_key : dxf piece key #asset assumed to be a ONE if piece key not given and we expand
        image_path: optional piece file
        size: resonance_size
        dxf_size: dxf_size
        color: resonance_color
        material: resonance_material
        compensation_x: stretch value
        compensation_y: stretch_value
        group_key: (optional) any unique code to split assets ino separate results
        group_piece_count: number of assets in the priority_group - should all be nested together (chk sum)
      # derived from the dxf / data but can be pre-cached
        geometry: optional - this is laded from dxf and provides the 300dpi outline for  nesting
        piece_file: optional - this is resolved from the asset attributes when not given
        labels: [xy, angle, provider] 
    }

    [args]:
    {
        callback_handler [integration test]
        plan
        cache_enabled
        #paginated args
        page: i
        num_pages
        approx_page_size: hint for page size, this is smoothed
        page_width
        page_height
        memory: XGi/Xmi
        file_parts: defaults to "nest_asset_print_page" which is where the pieces are put but can be anything
    }
    

    #example args for CTW70
    #setting these args is super important as we can vilate physical properties without them
    event['args']["stretch_x"] = 1.0256410256410258
    event['args']["stretch_y"] = 1.0101010101010102
    event['args']['output_bounds_width'] = 17100
----------------------
TODO: determine failure contract i.e. failure reason tags at asset level 
"""

from uuid import uuid1
import res
from res.flows.FlowContext import FlowContext
from res.flows import flow_node_attributes, payload_template
from res.learn.optimization.nest import (
    nest_dataframe,
    paginate_nested_dataframe,
    evaluate_nesting,
    tile_page,
    nest_dataframe_by_sets,
)

from res.media.images import compositor, text, make_header
from res.media.images.providers import DxfFile
from res.flows.dxa.res_color import (
    overlay_factory,
    res_plus_overlay_factory,
    unpack_res_color,
)


PAGE_MEMORY = 50

# construct a mappable payload - this feels a bit clumsy
def _job(p, pages, event, page_width, page_height):
    args = dict(event["args"])
    args.update(
        {
            "page": p,
            "num_pages": pages,
            "page_width": page_width,
            "page_height": page_height,
            "memory": f"{PAGE_MEMORY}Gi",
        }
    )

    return {
        "task": event["task"],
        "metadata": event["metadata"],
        "args": args,
        "apiVersion": "v0",
    }


# start small with the memory and see what we need (dataframe of shapes)
# - read the dxf files in generator and concat only what we need
@flow_node_attributes(memory="4Gi")
def generator(event, context=None):
    """
    Perform the nesting which generates a number of "pages" that are composited separately and then joined in the reducer
    Because this performs the logical nesting it can also be called to only plan the nesting but not apply it

    - Determines the number of pages / memory ahd chunking to send to the handlers after performing a nest
    - The payload is similar to the actual handler's but not paginated

    - adding a multi nest dimension to the pagination. We now have group id and page id, so nesting can do separate nests each in its own world coords

    TODO: the first two steps of this could be change for streaming and caching - as we stream assets, we can "revise" a cached nesting
    This revision is based on the logical groups. Nesting creates many packings in groups and if any if the groups change, we renest just those groups
    The availability of a roll that was assigned also can invalidate groups
    """

    from res.media.images.geometry import invert_axis

    with FlowContext(event, context) as fc:
        # whatever we fetch from files here could be stored in some database in future
        # key | asset_key | piece_type | material | color | file | geometry | resonance_color_inches
        data = fc.assets_dataframe

        data = fc.apply(
            "nested_asset_set",
            # nest_dataframe_by_sets,
            nest_dataframe,
            data,
            key=fc.key,
            sets=[["block_fuse"], ["self"]],
        )

        # we tile here in case there are multiple pages so we can map over them
        data = fc.apply("paginated_nest", paginate_nested_dataframe, data, key=fc.key)
        # should maybe do the eval as part of earlier steps - seems redundant here
        data = fc.apply("evaluated_nest", evaluate_nesting, data, key=fc.key)

        pages = int(data["chunks"].min())
        return [
            _job(
                p,
                pages,
                event,
                int(data["total_nested_width"].min()),
                int(data["tile_height"].min()),
            )
            for p in range(pages)
        ]


@flow_node_attributes(memory="50Gi")
def handler(event, context=None):
    """
    the handler gets the pages from payload or just prints all (1 page)
    the chunk size is a parameter which we default
     approx_page_size:
     page:
     (new) group_id <- this is a world parameter for a specifc physical assignment
     page_height: of the page
     page_width: of the page
     memory: can be passed but we try to fit on a fixed size

    When we match the locations and images we make the partial print file which is joined in the reducer

    # TODO add resonance color info to page e.g. outline thickness and label locations (offset)
    We make the logical page but it is only when we are loading

    fc.key is request_uid/roll_segment_id - if roll _segment id is * we can par over all of them anyway
    BUT rarely do that as the reduce cannot handle it - instead we just make N requests for each roll segment that has been planned
    Slight concern about the brittleness of the plan if the entire thing can be invalidated by a small change so we will protect against that

    Contaract:
    - this is the only place we need res color so if we want to avoid passing it around we can look it up here
    - the outline is the other complex type used here but we are unmarshalling geometries (TODO: res.color serialization)

    """

    text.ensure_s3_fonts()

    with FlowContext(event, context) as fc:
        page = fc.args.get("page", 0)
        page_key = f"{fc.key}/{page}"
        res.utils.logger.debug(f"processing page {page_key}")
        data = fc.get_node_data_by_index("paginated_nest", keys=fc.key)
        # load res color for keys

        data = tile_page(data, page)
        data = unpack_res_color(data)

        # TODO how do we know when to save a single item or a list of items
        fc.apply(
            "paginated_nest_composited",
            compositor.composite_images,
            data,
            key=page_key,
            dpi=(300, 300),
            res_color_fn_factory=res_plus_overlay_factory,
        )


@flow_node_attributes(memory="50Gi")
def reducer(event, context=None):
    """
    From the nest_asset_print_page we have an s3 folder with one or more parts
    We merge those together into a mosaic with pyvips

    Another dimension of nesting is we will now have multiple output groups e.g. rollids
    Nesting can now create multiple print files based on a grouping logic
    When the nest plans are constructed, we resolve roll segments and nest within available roll segments
    The trivial nest can nest one page on one roll but in general,
    - we first split the nest in to world coordinates on aviable roles
    - we then paginate within the world roll

    The reducer creates as many print files as there are world groups
    Plans can be confirmed one by one and the transaction commits

    TODO: in terms of efficiency we need to think about how multiple world print files can be done in parallel taking into account a printer bottleneck anyway
    #actually this can be easily handled by just adding a group filter to the call here. So we can commit one group at a tie

    fc.key format would be e.g request_uid/roll_segment_id which allows any print file for a grouped nesting to be printed
    """

    text.ensure_s3_fonts()

    with FlowContext(event, context) as fc:
        output_path_full = f"{fc.get_node_path_for_key('perform_nest_apply',fc.key)}/printfile_composite.png"
        # we can load files from an flow node - if we overide args with any list we can composite anything
        file_part_location_node = fc.args.get("file_parts", "paginated_nest_composited")
        parts = fc.get_node_file_names(
            file_part_location_node, fc.key, sorter="file_index"
        )

        header = make_header(fc.key, git_hash=fc.git_hash)

        # even for one we should composite headers and footers
        # if len(parts) == 1:
        #     res.utils.logger.info(
        #         f"There is only one part to the print file - copying directly to {output_path_full}"
        #     )
        #     fc.connectors["s3"].copy(parts[0], output_path_full)
        # else:

        res.utils.logger.info(f"uploading composite to {output_path_full}")
        compositor.join_images(parts, output_path_full, header_image=header)


def on_failure(event, context=None):
    with FlowContext(event, context) as fc:
        # call the callback handler
        pass


def on_success(event, context=None):
    with FlowContext(event, context) as fc:
        # call the callback handler
        pass


def _payload_template(version="resmagic.io/v1", **kwargs):
    p = payload_template("make.print_asset_sets", version, **kwargs)

    p["assets"] = [
        {
            "key": "10101010",
            "value": "body_piece_key",
            "piece_type": "self",
            "geometry": "POLYGON()",
            "resonance_color_inches": {},
            "body": "ABC123",
            "material": "ABC123",
            "color": "ABC123",
        }
    ]
    p["args"] = {
        "stretch_x": 1.0,
        "stretch_y": 1.0,
        "output_bounds_width": 16500,
        "output_bounds_length": 300000
        # todo: manage inches conversion for bounds if _inches is used for any measure
    }
    # sample args
    return p


# def _resolve_piece_files(fc):
#     # in future the file names should be on the asset dataframe and we remove next two lines
#     keys_from_assets = [a["value"].split("/")[-1].split(".")[0] for a in fc.assets]
#     # actually we are going to look up the files for these assets somehow - these point to new locations on S3
#     return fc.get_node_file_names("extract_parts", keys_from_assets)


# takeways:
# -we need a font strategy
