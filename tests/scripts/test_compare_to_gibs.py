import numpy as np
from compare_to_gibs import (
    NO_DATA,
    OFF_PALETTE,
    color_bins,
    parse_colormap,
    screen,
    value_bins,
)

COLORMAP = b"""<ColorMaps>
  <ColorMap title="No Data"><Entries>
    <ColorMapEntry rgb="0,0,0" transparent="true" nodata="true" ref="0" />
  </Entries></ColorMap>
  <ColorMap title="Nitrogen Dioxide"><Entries>
    <ColorMapEntry rgb="1,1,1" sourceValue="[0)" value="[0)" ref="1" />
    <ColorMapEntry rgb="2,2,2" value="[0,1.0e+15)" ref="2" />
    <ColorMapEntry rgb="3,3,3" value="[1.0e+15,2.0e+15)" ref="3" />
  </Entries></ColorMap>
</ColorMaps>"""


def test_value_and_color_bins_agree():
    edges, colors = parse_colormap(COLORMAP)
    assert edges.tolist() == [0, 1e15, 2e15]
    assert colors.tolist() == [[1, 1, 1], [2, 2, 2], [3, 3, 3]]

    values = np.array([-1, 0, 5e14, 1e15, 9e15, np.nan])
    assert value_bins(values, edges).tolist() == [0, 1, 1, 2, 2, NO_DATA]

    rgba = np.array([[[1, 1, 1, 255], [3, 3, 3, 255], [9, 9, 9, 255], [3, 3, 3, 0]]])
    assert color_bins(rgba.astype(np.uint8), colors).tolist() == [
        [0, 2, OFF_PALETTE, NO_DATA]
    ]


def test_screen_drops_flagged_and_cloudy_pixels():
    values = np.array([1.0, 2.0, 3.0, 4.0])
    flag = np.array([0, 1, 2, 0])
    cloud = np.array([0.1, 0.4, 0.1, 0.5])
    np.testing.assert_array_equal(
        screen(values, flag, cloud), [1.0, 2.0, np.nan, np.nan]
    )
