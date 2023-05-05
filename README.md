# Solar Data Generator

This tool will generate sample solar farm data using the following JSON schema:

```
{
    "device_id":  "device_01"  /* String, default 250 use -devices to set */
    "group_id": 12             /* integer, buckets into groups of 10 */
    "timestamp":  "2023-10-12 10:00:00" /* String in date/time format */
    "event_type": 0,          /* 0 - datapoint or 1 - error, if 1, add “event_details”:”Network error” occurs 0.05% of the time */
    "maxwatts": 250           // fixed at 250.
    "obs": [ 
         { "watts": 241 },              // integer. Initial value set 235-245
         { "amps": 2 },                 // decimal. Initial valie set 2-2.5 random.
         { "temp": 12 },                // integer. Initial value set 5-25 (in C)
        
     } 
}
```

## Installation

Install the kafka-python via:

`pip install -r requirements.txt`

Run the python app via:

`python solar.py`

Options:

| Parameter  | Description |
| ------------- |:-------------:|
| -d     | Number of devices (default 250)     |
| -kb    | Kafka Bootstrap Server (default='localhost:9092')     |
| -kt    | Topic name (default='solar')    |
| -x     | Number of minutes to generate (default 0=forever)    |

