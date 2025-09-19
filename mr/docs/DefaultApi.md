# openapi_client.DefaultApi

All URIs are relative to *http://localhost*

| Method                                             | HTTP request                  | Description          |
| -------------------------------------------------- | ----------------------------- | -------------------- |
| [**cancel_job**](DefaultApi.md#cancel_job)         | **POST** /job/cancel/{job_id} | Cancel a running job |
| [**get_job_result**](DefaultApi.md#get_job_result) | **GET** /job/result/{job_id}  | Get job result       |
| [**get_job_status**](DefaultApi.md#get_job_status) | **GET** /job/status/{job_id}  | Get job status       |
| [**health_check**](DefaultApi.md#health_check)     | **GET** /                     | Health Check         |
| [**submit_job**](DefaultApi.md#submit_job)         | **POST** /job/submit          | Submit a new job     |


# **cancel_job**
> CancelJob200Response cancel_job(job_id)

Cancel a running job

### Example


```python
import openapi_client
from openapi_client.models.cancel_job200_response import CancelJob200Response
from openapi_client.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = openapi_client.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with openapi_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = openapi_client.DefaultApi(api_client)
    job_id = 'job_id_example' # str |

    try:
        # Cancel a running job
        api_response = api_instance.cancel_job(job_id)
        print("The response of DefaultApi->cancel_job:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->cancel_job: %s\n" % e)
```



### Parameters


| Name       | Type    | Description | Notes |
| ---------- | ------- | ----------- | ----- |
| **job_id** | **str** |             |

### Return type

[**CancelJob200Response**](CancelJob200Response.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description                     | Response headers |
| ----------- | ------------------------------- | ---------------- |
| **200**     | Job cancelled                   | -                |
| **404**     | Job not found                   | -                |
| **409**     | Job already completed or failed | -                |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_job_result**
> GetJobResult200Response get_job_result(job_id)

Get job result

### Example


```python
import openapi_client
from openapi_client.models.get_job_result200_response import GetJobResult200Response
from openapi_client.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = openapi_client.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with openapi_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = openapi_client.DefaultApi(api_client)
    job_id = 'job_id_example' # str |

    try:
        # Get job result
        api_response = api_instance.get_job_result(job_id)
        print("The response of DefaultApi->get_job_result:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->get_job_result: %s\n" % e)
```



### Parameters


| Name       | Type    | Description | Notes |
| ---------- | ------- | ----------- | ----- |
| **job_id** | **str** |             |

### Return type

[**GetJobResult200Response**](GetJobResult200Response.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description      | Response headers |
| ----------- | ---------------- | ---------------- |
| **200**     | Result available | -                |
| **202**     | Result not ready | -                |
| **404**     | Job not found    | -                |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_job_status**
> GetJobStatus200Response get_job_status(job_id)

Get job status

### Example


```python
import openapi_client
from openapi_client.models.get_job_status200_response import GetJobStatus200Response
from openapi_client.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = openapi_client.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with openapi_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = openapi_client.DefaultApi(api_client)
    job_id = 'job_id_example' # str |

    try:
        # Get job status
        api_response = api_instance.get_job_status(job_id)
        print("The response of DefaultApi->get_job_status:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->get_job_status: %s\n" % e)
```



### Parameters


| Name       | Type    | Description | Notes |
| ---------- | ------- | ----------- | ----- |
| **job_id** | **str** |             |

### Return type

[**GetJobStatus200Response**](GetJobStatus200Response.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description         | Response headers |
| ----------- | ------------------- | ---------------- |
| **200**     | Job status response | -                |
| **404**     | Job not found       | -                |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **health_check**
> HealthCheck200Response health_check()

Health Check

### Example


```python
import openapi_client
from openapi_client.models.health_check200_response import HealthCheck200Response
from openapi_client.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = openapi_client.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with openapi_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = openapi_client.DefaultApi(api_client)

    try:
        # Health Check
        api_response = api_instance.health_check()
        print("The response of DefaultApi->health_check:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->health_check: %s\n" % e)
```



### Parameters

This endpoint does not need any parameter.

### Return type

[**HealthCheck200Response**](HealthCheck200Response.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description      | Response headers |
| ----------- | ---------------- | ---------------- |
| **200**     | API is reachable | -                |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **submit_job**
> SubmitJob201Response submit_job(submit_job_request)

Submit a new job

### Example


```python
import openapi_client
from openapi_client.models.submit_job201_response import SubmitJob201Response
from openapi_client.models.submit_job_request import SubmitJobRequest
from openapi_client.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = openapi_client.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with openapi_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = openapi_client.DefaultApi(api_client)
    submit_job_request = openapi_client.SubmitJobRequest() # SubmitJobRequest |

    try:
        # Submit a new job
        api_response = api_instance.submit_job(submit_job_request)
        print("The response of DefaultApi->submit_job:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->submit_job: %s\n" % e)
```



### Parameters


| Name                   | Type                                        | Description | Notes |
| ---------------------- | ------------------------------------------- | ----------- | ----- |
| **submit_job_request** | [**SubmitJobRequest**](SubmitJobRequest.md) |             |

### Return type

[**SubmitJob201Response**](SubmitJob201Response.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description            | Response headers |
| ----------- | ---------------------- | ---------------- |
| **201**     | Job accepted           | -                |
| **400**     | Invalid job submission | -                |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)
