from pyspark.statcounter import StatCounter

__author__ = 'Andrey Mironoff'
import itertools


class Chapter2Helpers:

    @staticmethod
    def isNotHeader(line):
        return "id_1" not in line

    @staticmethod
    # Convert the entire line into a tuple
    def parse(line):
        pieces = line.split(",")
        id1 = int(pieces[0])
        id2 = int(pieces[1])
        rawScores = pieces[2:11]
        matchingScores = map(lambda x: None if x == "?" else float(x), rawScores)
        matched = pieces[11] == 'TRUE'

        return MatchData(id1, id2, matchingScores, matched)


class MatchData:
    @property
    def x1(self):
        return self._x1

    @property
    def x2(self):
        return self._x2

    @property
    def scores(self):
        return self._scores

    @property
    def matched(self):
        return self._matched

    def __init__(self, x1, x2, scores, matched):
        self.x1 = x1
        self.x2 = x2
        self.scores = scores
        self.matched = matched

    def __repr__(self):
        return "X1: {0}, X2: {1}, Scores: {2}, Is Match: {3}".format(self.x1, self.x2, self.scores, self.matched)


class NAStatCounter:

    def __init__(self):
        self.stats = StatCounter()
        self.missing = long(0)

    def add(self, x):
        if x is None:
            self.missing += 1
        else:
            self.stats.merge(x)

        return self

    def mergeStats(self, other):

        self.stats.mergeStats(other.stats)
        self.missing += other.missing

        return self

    def __repr__(self):
        return "stats: {0}, NaN: {1}".format(self.stats, self.missing)