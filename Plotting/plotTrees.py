# -*- coding: utf-8 -*-

import sys
from Code.environment import filePath

import subprocess
from IPython.display import Image

# %% variables

mojo_file_name = filePath + "/06_Models/drf_final_text.mojo.zip"
h2o_jar_path = "/Users/janschaefer/Downloads/h2o-3.18.0.11/h2o.jar"
mojo_full_path = mojo_file_name
gv_file_path = filePath + "/06_Models/drf_final_text.gv"

# %% set destination

image_file_name = filePath + "/06_Models/drf_final_text_plot_"


# %% define
def generateTree(
    h2o_jar_path, mojo_full_path, gv_file_path, image_file_path, tree_id=0
):
    image_file_path = image_file_path + "_" + str(tree_id) + ".png"
    result = subprocess.call(
        [
            "java",
            "-cp",
            h2o_jar_path,
            "hex.genmodel.tools.PrintMojo",
            "--tree",
            str(tree_id),
            "-i",
            mojo_full_path,
            "-o",
            gv_file_path,
        ],
        shell=False,
    )
    result = subprocess.call(["ls", gv_file_path], shell=False)
    if result is 0:
        print("Success: Graphviz file " + gv_file_path + " is generated.")
    else:
        print("Error: Graphviz file " + gv_file_path + " could not be generated.")


def generateTreeImage(gv_file_path, image_file_path, tree_id):
    image_file_path = image_file_path + "_" + str(tree_id) + ".png"
    result = subprocess.call(
        ["dot", "-Tpng", gv_file_path, "-o", image_file_path], shell=False
    )
    result = subprocess.call(["ls", image_file_path], shell=False)
    if result is 0:
        print("Success: Image File " + image_file_path + " is generated.")
        print("Now you can execute the follow line as-it-is to see the tree graph:")
        print("Image(filename='" + image_file_path + "')")
    else:
        print("Error: Image file " + image_file_path + " could not be generated.")


# %% generate
generateTree(h2o_jar_path, mojo_full_path, gv_file_path, image_file_name, 1)

# %% convert

generateTreeImage(gv_file_path, image_file_name, 3)
