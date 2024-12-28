Parse the timeframe from this query: "${text}"
Return a JSON object with unit and amount fields.
Valid units are: hours, days, weeks, months

Rules:
- For "week" or "weekly", use unit: weeks
- For "month" or "monthly", use unit: months
- For "day" or "daily", use unit: days
- For "hour" or "hourly", use unit: hours
- If no specific amount is mentioned with the unit, use amount: 1

Examples:
"last 24 hours" -> {"unit": "hours", "amount": 24}
"past week" -> {"unit": "weeks", "amount": 1}
"last 7 days" -> {"unit": "days", "amount": 7}
"past month" -> {"unit": "months", "amount": 1}
"for the past week" -> {"unit": "weeks", "amount": 1}
"over the last month" -> {"unit": "months", "amount": 1}

Return only the JSON response without any additional text or explanation.
Format: {"unit": "unit_name", "amount": number} 