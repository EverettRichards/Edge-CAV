config = {
    "broker_IP":"localhost",
    "port_Num":1883,
    "verdict_min_refresh_time": 0.15, # Min number of seconds before a new verdict can be submitted
    "oldest_allowable_data": 10, # Max number of seconds before data is considered too old
    "show_verbose_output": True,
    "reputation_increment": 0.005, # Amount to increment or decrement client reputation by when they make a right decision
    "reputation_decrement": 0.010, # Amount to decrement client reputation by when they make a wrong decision
    "min_reputation": 0.35, # Minimum reputation value
}