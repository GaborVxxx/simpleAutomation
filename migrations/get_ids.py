import json
import time

def get_ids():
    # Simulate a delay for the API call
    time.sleep(4)
    # For demonstration purposes, generate 100 IDs.
    return [i for i in range(1, 101)]

def main():
    ids = get_ids()
    # Print the IDs as a JSON string so the main script can parse it
    print(json.dumps(ids))

if __name__ == "__main__":
    main()