import pytest
import freppy

base_dir = "/Users/krasting/PycharmProjects/freppy/pp"
catalog = freppy.catalog_from_dir(base_dir)


# -- below are some extreme test cases
# base_dir = "/archive/jpk/fre/siena_201204/ESM2G/ESM2G-C2_1pct-co2_4x_U2/gfdl.nescc-default-prod-openmp/pp/"
# base_dir = "/archive/jpk/fre/siena_201204/ESM2G/ESM2G_pi-control_C2/gfdl.nescc-default-prod-openmp/pp/"

# testing
# path_to_pp = dl.dora_metadata("odiv-210")["pathPP"] + "ocean_annual_z/"
##get_ipython().run_cell_magic('time', '', '\ncatalog = catalog_from_dir(path_to_pp, filename="odiv-210")\n')
# catalog[catalog.index==0]
# cat = intake.open_esm_datastore("odiv-210.json")
# subset = cat.search(variable_id="thetao",gfdl_component="ocean_annual_z")
# subset
# ds = subset.to_dataset_dict()
