#!/bin/bash
# usage: runall.sh <year> [po]   -> parses nbastats_<year> (or nbastats_po_<year>) into data/out (out_po)
y=$1; tag=${2:+_po}; o=/home/claude/data/out${2:+_po}
d=/home/claude/data/tmp${tag}_$y; mkdir -p $d
tar -xJf /home/claude/shufinskiy/nba_data/datasets/nbastats${tag}_$y.tar.xz -C $d
python3 /home/claude/work/parse.py $y $d/nbastats${tag}_$y.csv $o
rm -rf $d
