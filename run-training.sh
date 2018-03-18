#!/bin/bash

conda env update

source activate assignment

python entity_resolution.py

source deactivate