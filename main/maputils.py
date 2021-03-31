import random
import folium # pip install folium
from folium import plugins

def setup_map(center, zoom_start = 14, filename = "draw.geojson"):
    """Create a basic folium map object"""
    map_ = folium.Map(
        location = center,
        zoom_start = zoom_start,
        tiles = "cartodbdark_matter",
    )
    plugins.Fullscreen(
        position="topleft"
    ).add_to(map_)
    plugins.Draw(
        filename=filename,
        export=True,
        position="topleft"
    ).add_to(map_)
    return map_

def rand_24_bit():
    return random.randrange(0, 16**6)

def color_dec():
    return rand_24_bit()

def color_hex(num=rand_24_bit()):
    return "%06x" % num

def color_rgb(num=rand_24_bit()):
    hx = color_hex(num)
    barr = bytearray.fromhex(hx)
    return (barr[0], barr[1], barr[2])