import pytest
import freppy

import pandas as pd
import shutil
import tempfile

# base_dir = "/Users/krasting/PycharmProjects/freppy/pp"


def test_functional_1():
    df_catalog = freppy.catalog_from_dir(pytest.ppdir)
    assert isinstance(df_catalog, pd.DataFrame)
    assert len(df_catalog) == 2659


def test_functional_2():
    df_catalog = freppy.catalog_from_dir(pytest.ppdir)
    consolidated = freppy.consolidate_monthly_av(df_catalog)
    assert len(consolidated) == 2494


def test_functional_3():
    df_catalog = freppy.catalog_from_dir(pytest.ppdir)
    consolidated = freppy.consolidate_monthly_av(df_catalog)
    exploded = freppy.infer_av_variables(consolidated)
    assert len(exploded) == 5968


# testing
# path_to_pp = dl.dora_metadata("odiv-210")["pathPP"] + "ocean_annual_z/"
##get_ipython().run_cell_magic('time', '', '\ncatalog = catalog_from_dir(path_to_pp, filename="odiv-210")\n')
# catalog[catalog.index==0]
# cat = intake.open_esm_datastore("odiv-210.json")
# subset = cat.search(variable_id="thetao",gfdl_component="ocean_annual_z")
# subset
# ds = subset.to_dataset_dict()
