# Basic program I made to mass scrape geo data on ip addresses for statistic purposes
# Async is kinda sloppy, I thought this would be a good chance to work with async workers

# Imports
from time import time
from aiofile import LineReader, AIOFile
import aiohttp
import asyncio

# Files
ip_list = "ips.txt"
result_list = "ip_results.csv"

# Setup the queue
ip_queue = asyncio.Queue()


# Async request function
async def fetch(session, url):
    async with session.get(url) as response:
        return await response.json()


# Load all items from file into queue
async def load_file(file, queue):
    # Start timer to use for statistics later
    start = time()

    # Open files using AIOFile to fully use async
    async with AIOFile(file) as afp:
        async for line in LineReader(afp):
            line = line[:-1]  # Tail-less string
            await queue.put(line)

    # End timer and check how long it took to put how many items in queue
    end = time()
    print(f"[QUEUE] Added {queue.qsize()} items to queue in {(end - start):.2f} seconds.")


# Run the function to check all ips in queue
async def check_ips(queue):
    # Start time to measure how long this process takes
    start = time()

    # As long as an item in queue it will loop
    while queue.qsize() > 0:
        # Define item in queue
        item = await queue.get()

        # Run the api call
        await ip_lookup(item, result_list)

        # After queue is done remove from queue
        queue.task_done()

    # End timer so we can subtract the start to see how long the process took
    end = time()
    print(f"[QUEUE] Finished checking all items in queue in {(end - start):.2f} seconds.")


# Function to actually lookup the IP address
async def ip_lookup(ip, output_file):

    # Setup async client session
    async with aiohttp.ClientSession() as session:
        try:
            # Make a request to the api and return json data
            data = await fetch(session, f"https://ipinfo.io/{ip}/json")

            # Variables for easy access to the data we're storing
            provider, ip_address = data["org"], data["ip"]
            country, region_name, city_name = data["country"], data["region"], data["city"]

            # Save data to "output_file" while separating lines each time
            async with AIOFile(output_file, "a") as afp:
                await afp.write(f"{ip_address},{provider},{city_name},{region_name},{country}\n")

            # Print the data being stored
            print(f"{ip_address},{provider},{city_name},{region_name},{country}")

        # Catch all errors and print what failed
        except KeyError as error:
            print(f"[ERROR]: failed {error} for {ip}")


if __name__ == "__main__":
    asyncio.run(load_file(file=ip_list, queue=ip_queue))
    asyncio.run(check_ips(queue=ip_queue))
