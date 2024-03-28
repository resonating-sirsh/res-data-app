import math


class SegmentTree:
    """
    A binary tree of line segments (x0, y0, x1, y1) which supports "stabbing" queries
    which return all the segments which contain some x value.
    """

    def __init__(self, segments):
        self.depth = int(math.ceil(math.log(len(segments), 2)))
        self.nodes = [None] * (2**self.depth - 1) + sorted(
            segments, key=lambda s: s[0] + s[2]
        )
        self.n = len(self.nodes)
        for d in range(self.depth - 1, -1, -1):
            for i in range(2**d):
                node = 2**d - 1 + i
                l = 2 * node + 1
                r = 2 * node + 2
                if l >= self.n or self.nodes[l] is None:
                    break
                if r >= self.n or self.nodes[r] is None:
                    self.nodes[node] = self.nodes[l]
                else:
                    self.nodes[node] = (
                        min(self.nodes[l][0], self.nodes[r][0]),
                        0,
                        max(self.nodes[l][2], self.nodes[r][2]),
                        0,
                    )

    def _filter(self, node, depth, predicate):
        if depth == self.depth:
            yield self.nodes[node]
        else:
            l = 2 * node + 1
            r = 2 * node + 2
            if l < self.n and self.nodes[l] is not None and predicate(self.nodes[l]):
                yield from self._filter(l, depth + 1, predicate)
            if r < self.n and self.nodes[r] is not None and predicate(self.nodes[r]):
                yield from self._filter(r, depth + 1, predicate)

    def crossing(self, x, tol=1e-6):
        pred = lambda node: node[0] < x + tol and node[2] + tol > x
        if pred(self.nodes[0]):
            yield from self._filter(0, 0, pred)

    def overlapping(self, x0, x1, tol=1e-6):
        pred = (
            lambda node: (node[0] < x0 and x1 < node[2])
            or (x0 - tol < node[0] and node[0] < x1 + tol)
            or (x0 - tol < node[2] and node[2] < x1 + tol)
        )
        if pred(self.nodes[0]):
            yield from self._filter(0, 0, pred)


class SegmentList:
    """
    The price to pay for the tree structure is not warranted when the number of segments is small.
    So this thing just wraps a sorted list and does the same thing.
    """

    def __init__(self, segments):
        self.segments = sorted(segments)

    def crossing(self, x, tol=1e-6):
        for seg in self.segments:
            if seg[0] > x + tol:
                break
            if seg[0] < x + tol and seg[2] + tol > x:
                yield seg

    def overlapping(self, x0, x1, tol=1e-6):
        for seg in self.segments:
            if seg[0] > x1 + tol:
                break
            if (
                (seg[0] < x0 and x1 < seg[1])
                or (x0 - tol < seg[0] and seg[0] < x1 + tol)
                or (x0 - tol < seg[2] and seg[2] < x1 + tol)
            ):
                yield seg


class SweeplineHelper:
    def __init__(self, segments):
        self.segments = sorted(segments)
        self.active = []
        self.idx = 0
        self.x = None

    def update(self, x, drop=True):
        """
        Update the active set of segments to include everything which begins at a point up to x.
        Optionally drop out all the segments which end before x.
        """
        if self.x is None or x > self.x:
            self.x = x
            while self.idx < len(self.segments) and self.segments[self.idx][0] <= x:
                self.active.append(self.segments[self.idx])
                self.idx += 1
            if drop:
                self.active = [seg for seg in self.active if seg[2] >= x]
