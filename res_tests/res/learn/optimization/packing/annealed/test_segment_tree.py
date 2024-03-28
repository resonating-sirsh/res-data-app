from res.learn.optimization.packing.annealed.segment_tree import (
    SegmentList,
    SegmentTree,
)


def test_segment_tree():
    segs = [(i, 0, i + 3, 0) for i in range(10)]
    st = SegmentTree(segs)
    for i in range(10):
        res = list(st.crossing(i))
        assert res == [(j, 0, j + 3, 0) for j in range(max(0, i - 3), i + 1)]
        res = list(st.overlapping(i, i + 2))
        assert res == [(j, 0, j + 3, 0) for j in range(max(0, i - 3), min(10, i + 3))]


def test_segment_list():
    segs = [(i, 0, i + 3, 0) for i in range(10)]
    st = SegmentList(segs)
    for i in range(10):
        res = list(st.crossing(i))
        assert res == [(j, 0, j + 3, 0) for j in range(max(0, i - 3), i + 1)]
        res = list(st.overlapping(i, i + 2))
        assert res == [(j, 0, j + 3, 0) for j in range(max(0, i - 3), min(10, i + 3))]
