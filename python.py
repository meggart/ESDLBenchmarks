from esdl import Cube
import xarray as xr
import dask
from timeit import timeit


def ESDLBenchmark(cubepath): 
  
  def openfun():
    if cubepath.endswith("zarr"):
      cube=xr.open_zarr(cubepath)
      ds = cube[["gross_primary_productivity","net_ecosystem_exchange","terrestrial_ecosystem_respiration"]].sel(lon=slice(-10,50),lat=slice(65,35))
    else:
      cube = Cube.open(cubepath)
      ds =   cube.data.dataset(key=["gross_primary_productivity","net_ecosystem_exchange","terrestrial_ecosystem_respiration"])
      ds = ds[["gross_primary_productivity","net_ecosystem_exchange","terrestrial_ecosystem_respiration"]].sel(lon=slice(-10,50),lat=slice(65,35))
    
    return(ds)

  ds = openfun()
  
  def mapaccessfun():
    a = ds.isel(time=99).compute()
    return a

  def tsaccessfun():
    a = ds.isel(lon=99,lat=99).compute()
    return a

  def mscfun():
    a = ds.groupby("time.dayofyear").mean(dim="time").compute()
    return a

  def removemscfun():
    a = (ds.groupby("time.dayofyear")-ds.groupby("time.dayofyear").mean(dim="time")).compute()
    return a

  def spatialmeanfun():
    a = ds.mean(dim=("lon","lat")).compute()
    return a

  tim = {}
  tim["cube"] = cubepath
  tim["open"] = timeit(openfun,number=1)
  tim["map_access"] = timeit(mapaccessfun,number=1)
  tim["ts_access"]  = timeit(tsaccessfun,number=1)
  tim["msc"]        = timeit(mscfun,number=1)
  tim["removeMSC"]  = timeit(removemscfun, number=1)
  tim["spatial_mean"] = timeit(spatialmeanfun,number=1)
  return tim

tim_old = ESDLBenchmark("/scratch/DataCube/v1.0.0/low-res")
tim_compressed_map = ESDLBenchmark("/scratch/DataCube/v1.0.1/esdc-8d-0.25deg-1x720x1440-1.0.1_1")
tim_compressed_ts  = ESDLBenchmark("/scratch/DataCube/v1.0.1/esdc-8d-0.25deg-46x90x90-1.0.1_2")
#tim_zarr           = ESDLBenchmark("/scratch/DataCube/v1.0.1/esdc-8d-0.25deg-1x720x1440-1.0.1_1_zarr")

all_timings = [tim_old,tim_compressed_map,tim_compressed_ts]
import csv

with open('python_benchmark.csv', 'w', newline='') as csvfile:
    fieldnames = all_timings[0].keys()
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for tim in all_timings:
      writer.writerow(tim)
