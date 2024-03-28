from botocore.retries.bucket import TokenBucket
from res.flows import FlowContext, legacy_load_airtable_data
import pandas as pd
from res.learn import optimization

# think about data storage with context and how data work with flows
# think about how solution is build up generically as data - is requested and omitted a general thing
# every node has an out to add [ {} ] of metadata e.g. sizes of pieces


def get_material_info(fc):
    material = fc.args["material"]
    at = fc.connectors["airtable"]
    filter = "FIND('" + material + "', {Material Code})"
    # load materials and map the values to this nodes parameters
    data = at.query_table_to_our_schema(fc, "materials", filter=filter).rename(
        columns={
            # TODO
        }
    )
    fc.args.update(dict(data.iloc[0]))


def get_conversions(fc):
    token = "_inches"
    for k, v in fc.args:
        if k[-len(token) :] == token:
            fc.args[k.replace(token, "")] = int(v * optimization.DPI_SETTING)


def apply_limits(fc):
    for k, v in fc.arg_upper_bounds.items():
        fc.args[k] = max(fc.args[k], v)


def perform_image_piece_extract(event, context):
    # if the flow context fails and there is an on_notify it will send the status back to the user
    # the node progress is constantly update in apply so this status can be sent
    with FlowContext(event, context) as fc:
        for asset in fc.assets:
            data = fc.connectors["s3"].read(asset.value)
            # the dag knows what function to use to do evaluation
            for node in fc.get_dag("perform_image_piece_extract"):
                # this determines the args internally and converts the data
                # the key must depend on the node i.e. is it global or something TBD
                data = fc.apply(**node, data=data, key=asset.key)


def perform_nest(event, context):
    with FlowContext(event, context) as fc:
        fc.input_args_pipeline(get_material_info, get_conversions, apply_limits)
        # fc.output_args_piplien() #delayed for notify
        data = None
        # the first node of this dag is the one the fetches the data from alpha nulls
        # the default dag is the one we use here in our iterator
        for node in fc.dag:
            # the node unpacks into a dict name, func, eval_func etc. basically anything apply needs to do its magic
            # internall we are going to have the nest function handle iterations
            # assets are things that nodes produced therefore the persistence and eventing is easy
            # topics will contain (accept) some of the fields so we can always publish the nodes assets if there is a topic/its configured
            # note when the asset is data, we store a handle to the data and save the data on S3
            data = fc.apply(**node, data=data, key=fc.job_key)
            # actually not sure how to create the output path properly
            # also the node has two inputs - how do you model that?
            # data should be a dictionary with the piece keys associated e.g. vectors and piece paths + original asset codes etc.
            # the print function can load its own generator from the input data - a content reader is passed in from the context


# meta do an analysis on sizes of rolls that are optimal
# for example given the size of a piece, there is a point where once there are enough of a distribution of sizes,
# we are good and we can not get more efficient because there is a sort of fractal repeat
def build_roll_segments(event, context):
    with FlowContext(event, context) as fc:
        # get the pieces and their attributes from a suitable query
        # we could load keypoints / size in memory / order date / one / retries and other statistics merged on
        # we could assume this is already split by feasible sets e.g. same material/type
        # we should add the compensations to here possibly? it make sense to have the option to get the asset headers out-of-band
        assets = fc.assets or fc.pull_assets()

        material = None

        # we should know what roll is available as a function of time: purchasing of rolls and printer data (locked rolls is the allocation | usage is what we use
        # I want to understand how much material is available and details about "child" rolls
        # i also want to understand throughput so i understand "scheduling" of bins
        # how long to roll change overs actual take
        bins = fc.optimizer.determine_bins(assets, material)

        segment_assignments = fc.optimizer.try_pack(assets, bins)

        # data in motion has structured and can be flattened in memory
        # so the new asset structure is like
        # [asset: { asset_attributes: {}, assigned_assets: { props } ,,,,,,,]
        # this can be expanded using a df with explode assigned asssets and extract other attributes
        fc.publish_assets(segment_assignments)

        # when commit the assets we must make sure the residual is known


# TODO:
# 1. store a dag in a database with res ops
# 2. create outs on the nodes and save them
# 3. create evaluation functions X_evaluation_fn
# 4. the dag needs to source from another node
