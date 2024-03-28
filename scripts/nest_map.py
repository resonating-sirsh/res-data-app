import res
from res.learn.optimization.packing.annealed.packing_geometry import choose_geom
from shapely.wkt import loads
from shapely.affinity import translate, scale


def simplified_shape(p):
    return p
    # shape = choose_geom(p)
    # return translate(shape.box, *(-shape.local_offset))


s3 = res.connectors.load("s3")

nest_df_path = "s3://res-data-production/flows/v1/make-nest-progressive-construct_rolls/primary/nest/construct-rolls-bernardo-1670441373/CTNOX-1670441501/CTNOX-1670441501_R19061-CTNOX_0_self.feather"
nest_composited_path = "s3://res-data-production/flows/v1/make-nest-stitch_printfiles/primary/composite/stitch-r19061--ctnox-1-1670441520/CTNOX-1670441501_R19061-CTNOX_0_self/printfile_composite_thumbnail.png"

nest_df = s3.read(nest_df_path)
original_composited_height = nest_df.composition_y.max() + 10
original_composited_width = nest_df.output_bounds_width.iloc[0]
img_height = min(int(original_composited_height / 5), 15000)
img_width = int(img_height * original_composited_width / original_composited_height)
img_href = s3.generate_presigned_url(nest_composited_path)
nest_width = nest_df.output_bounds_width.max()
nest_height = nest_df.max_nested_y.max()
scale_x = img_height / nest_height
scale_y = img_width / nest_width

nest_df["nested_geometry"] = nest_df["nested_geometry"].apply(loads)
nest_df["simplified_geom"] = nest_df.nested_geometry.apply(simplified_shape)
nest_df["y_coord"] = (nest_df.max_nested_y + nest_df.min_nested_y) / 2
nest_df["simplified_geom"] = nest_df.apply(
    lambda r: translate(r.simplified_geom, 0, nest_height - 2 * r.y_coord),
    axis=1,
)
nest_df["simplified_geom"] = nest_df["simplified_geom"].apply(lambda p: scale(p, 1, -1))

# old assets
if "piece_name" not in nest_df.columns:
    nest_df["piece_name"] = None
nest_df["piece_name"] = nest_df.where(
    nest_df.piece_name.notnull(),
    nest_df.s3_image_path.apply(lambda p: p.split("/")[-1].replace(".png", "")),
    axis=0,
)

map_pieces = [
    f"""
    <area
        href='#'
        piece_id='{r.piece_id}'
        shape='poly'
        coords='{",".join([f"{int(x*scale_x)},{int(y*scale_y)}" for x, y in r.simplified_geom.boundary.coords][:-1])}'    
    />
    """
    for _, r in nest_df.iterrows()
]

piece_info = ",".join(
    f"""'{r.piece_id}': {r[["asset_key", "asset_id", "piece_name", "min_nested_x", "min_nested_y"]].to_json()}"""
    for _, r in nest_df.iterrows()
)

imagemap_js = """
function resize() {
    $('img').mapster('resize', $(window).width()/2, 0 ,0);   
}

function onWindowResize() {
    var curWidth = $(window).width(),
        curHeight = $(window).height(),
        checking = false;
    if (checking) {
        return;
    }
    checking = true;
    window.setTimeout(function() {
        var newWidth = $(window).width(),
           newHeight = $(window).height();
        if (newWidth === curWidth &&
            newHeight === curHeight) {
            resize(); 
        }
        checking=false;
    }, 100);
}


$(document).ready(function () {
    $('img').mapster({
        fillColor: 'ff0000',
        stroke: true,
        singleSelect: true,
        mapKey: 'piece_id',
        onClick: function (e) {
            $('#piece-id').html(e.key);
            $('#piece-name').html(piece_info[e.key]["piece_name"]);
            $('#asset-key').html(piece_info[e.key]["asset_key"]);
            console.log(piece_info[e.key]);
            console.log(piece_info[e.key]["piece_name"]);
            console.log(piece_info[e.key].piece_name);
            var a = piece_info[e.key];
            console.log(a);
            console.log(a["piece_name"]);
            console.log(a.piece_name);
        },
    });
    $(window).bind('resize', onWindowResize);
    resize();
});
"""

html = f"""
<html>
    <head>
        <script type='text/javascript' src='https://ajax.googleapis.com/ajax/libs/jquery/3.5.1/jquery.min.js'></script>
        <script type='text/javascript' src='https://cdn.jsdelivr.net/npm/imagemapster@1.5.4/dist/jquery.imagemapster.min.js'></script>
    </head>
    <body>
        <img src='{img_href}' usemap='#nest-map'/>
        <map id='nest-map' name='nest-map'>
        {' '.join(map_pieces)}
        </map>
        <div id='info-box' name='info-box' style='position:fixed;left:55%;top:5%;font-size:25;font-family:helvetica'>
            <b>Piece id:</b> <div id='piece-id' style='display:inline'></div><br/>
            <b>Asset Key:</b> <div id='asset-key' style='display:inline'></div><br/>
            <b>Piece Name:</b> <div id='piece-name' style='display:inline'></div><br/>
        </div>
        <script type='text/javascript'>
            var piece_info = {{{piece_info}}};
            {imagemap_js}
        </script>
    </body>
</html>
"""

with open("map.html", "w") as f:
    f.write(html)
