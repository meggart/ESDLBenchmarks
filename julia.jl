using OnlineStats
using ESDL
using Zarr
using ESDLZarr

function runbenchmark(cubedir;max_cache=1e7)
  
  tim = Dict{String,Any}()
  tim["cube"]=cubedir
  tim["cache_size"]=max_cache
  tim["open"] = @elapsed begin
    c = endswith(cubedir,"zarr") ? ESDLZarr.ZarrCube(cubedir) : Cube(cubedir)
    d=getCubeData(c,variable = ["gross_primary_productivity","net_ecosystem_exchange","terrestrial_ecosystem_respiration"],longitude=(-10,50),latitude=(35,65))
  end
  tim["map_access"] = @elapsed a = d[:,:,100,:]
  tim["ts_access"]  = @elapsed a = d[100,100,:,:]
  tim["msc"] = @elapsed getMSC(d,max_cache=max_cache)
  tim["removeMSC"] = @elapsed removeMSC(d,max_cache=max_cache)
  tim["country_mean"] = @elapsed begin
    countries = getCubeData(c,variable="country_mask",longitude=(-10,50),latitude=(35,65))
    mapCube(Mean,d,by=(VariableAxis,countries),max_cache=max_cache)
  end
  tim["spatial_mean"]=@elapsed mapCube((xout,xin)->xout[1] = mean(filter(!isnan,xin)),d,indims=InDims("Lon","Lat",miss=ESDL.NaNMissing()),outdims=OutDims(miss=ESDL.NaNMissing()),max_cache=max_cache)
  tim
end

tim_precompile = runbenchmark("/scratch/DataCube/v1.0.0/low-res")
tim_old = runbenchmark("/scratch/DataCube/v1.0.0/low-res")
tim_compressed_map = runbenchmark("/scratch/DataCube/v1.0.1/esdc-8d-0.25deg-1x720x1440-1.0.1_1")
tim_zarr_map = runbenchmark("/scratch/DataCube/v1.0.1/esdc-8d-0.25deg-1x720x1440-1.0.1_1_zarr")
tim_compressed_ts = runbenchmark("/scratch/DataCube/v1.0.1/esdc-8d-0.25deg-46x90x90-1.0.1_2")

#Now with more memory
tim_old_c = runbenchmark("/scratch/DataCube/v1.0.0/low-res",max_cache=1e8)
tim_compressed_map_c = runbenchmark("/scratch/DataCube/v1.0.1/esdc-8d-0.25deg-1x720x1440-1.0.1_1",max_cache=1e8)
tim_zarr_map_c = runbenchmark("/scratch/DataCube/v1.0.1/esdc-8d-0.25deg-1x720x1440-1.0.1_1_zarr",max_cache=1e8)
tim_compressed_ts_c = runbenchmark("/scratch/DataCube/v1.0.1/esdc-8d-0.25deg-46x90x90-1.0.1_2",max_cache=1e8)

all_tims = [tim_old, tim_compressed_map,tim_compressed_ts,tim_old_c,tim_compressed_map_c,tim_compressed_ts_c,tim_zarr_map, tim_zarr_map_c]
colnames = collect(keys(all_tims[1]))
v = map(colnames) do k
  [d[k] for d in all_tims]
end
using DataFrames
using CSV
df = DataFrame(Dict(zip(colnames,v)))
CSV.write("julia_benchmarks.csv",df,header=true)
  


