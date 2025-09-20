# CancelJob200Response


## Properties

| Name       | Type    | Description | Notes      |
| ---------- | ------- | ----------- | ---------- |
| **job_id** | **str** |             | [optional] |
| **status** | **str** |             | [optional] |

## Example

```python
from openapi_client.models.cancel_job200_response import CancelJob200Response

# TODO update the JSON string below
json = "{}"
# create an instance of CancelJob200Response from a JSON string
cancel_job200_response_instance = CancelJob200Response.from_json(json)
# print the JSON string representation of the object
print(CancelJob200Response.to_json())

# convert the object into a dict
cancel_job200_response_dict = cancel_job200_response_instance.to_dict()
# create an instance of CancelJob200Response from a dict
cancel_job200_response_from_dict = CancelJob200Response.from_dict(cancel_job200_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
