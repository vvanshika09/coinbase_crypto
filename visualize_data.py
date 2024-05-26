import boto3
from boto3.dynamodb.conditions import Key
import matplotlib.pyplot as plt
from datetime import datetime
import matplotlib.dates as mdates

# DynamoDB setup
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table_name = 'BitcoinPrices'
table = dynamodb.Table(table_name)

def retrieve_data_from_dynamodb():
    try:
        response = table.scan()
        items = response['Items']
        return items
    except Exception as e:
        print(f"Error retrieving data from DynamoDB: {e}")
        return []

def plot_bitcoin_prices(data):
    timestamps = []
    prices = []
    
    for item in data:
        # Assuming the timestamp is a string, convert it to a datetime object for plotting
        dt = datetime.strptime(item['timestamp'], '%Y-%m-%d %H:%M:%S')
        timestamps.append(dt)
        prices.append(float(item['price']))  # Convert price from string to float
    
    # Sort data by timestamps
    sorted_data = sorted(zip(timestamps, prices))
    sorted_timestamps, sorted_prices = zip(*sorted_data)
    
    # Extract HH:MM for x-axis
    times = [dt.strftime('%H:%M') for dt in sorted_timestamps]
    
    # Extract unique dates for the legend
    unique_dates = sorted(set(dt.strftime('%Y-%m-%d') for dt in sorted_timestamps))

    # Plotting
    plt.figure(figsize=(10, 6))
    plt.plot(times, sorted_prices, linestyle='-')
    
    plt.xlabel('Time')
    plt.ylabel('Price (USD)')
    plt.title('Bitcoin Prices Over Last Hour')
    plt.xticks(rotation=45, fontsize=6)  # Rotate x-axis labels and set font size
    plt.grid(True)
    plt.legend(unique_dates, title="Date (UTC)")
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    data = retrieve_data_from_dynamodb()
    if data:
        plot_bitcoin_prices(data)
    else:
        print("No data to plot.")
