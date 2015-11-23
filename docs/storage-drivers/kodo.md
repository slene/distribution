<!--[metadata]>
+++
title = "KODO storage driver"
description = "Explains how to use the KODO storage drivers"
keywords = ["registry, service, driver, images, storage,  KODO"]
+++
<![end-metadata]-->


# KODO storage driver

An implementation of the `storagedriver.StorageDriver` interface which uses Qiniu Cloude KODO for object storage.

## Parameters

`accesskey`: Your kodo access key.

`secretkey`: Your kodo secret key.

`zone`: Your storage zone index number (default 0).

`bucket`: The name of your kodo bucket where you wish to store objects (needs to already be created prior to driver initialization).

`baseurl`: The url of your kodo bucket domain use to fetch files.

`rootdirectory`: (optional) The root directory tree in which all registry files will be stored. Defaults to the empty string (bucket root).

`rshost`: (optional) Server address of RS service.

`rsfhost`: (optional) Server address of RSF service.

`iohost`: (optional) Server address of IO service.

`uphosts`: (optional) Server addresses of UP service.
