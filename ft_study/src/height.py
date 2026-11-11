"""Does player height predict the xFoul residual? Heights/weights from Basketball-Reference's player index (26 pages,
fetched once to data/ext/bbref/*.html), matched by normalized name + career overlap (99.3% of attempts).
Outputs: slopes of fouls-over-expected per 100 attempts on height (overall, by zone, with weight / rim share), and a
height-adjusted REAVES (xF + zone-specific height slope x (height - zone mean height)): results/reaves_height_adjusted.csv.
(Run interactively Nov 11; this file records the method. The match step wrote data/ext/heights.csv.)"""
