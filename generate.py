import csv
import os
import random
import shutil
import time
import sys
import pandas as pd

# Path to save the generated data to be consumed
PATH = "./input_data/"
# Range for the minimum and maximum number of changes happening each cycle
CHANGES_SIZE = (5, 10)
# Time to pause after each cycle
SLEEP_TIME = 5

# Path to save the output data
PATH_OUT = "./output_data/"

# Generate updates based on the existing data
def gen_updates(data, size):
    # Update a random set of elements
    # "size" is the number of elements being updated
    samples_indexes = random.sample(range(len(data)), size)
    events = []
    for i in samples_indexes:
        data[i]["op"] = "u"
        # Balance delta can be either positive or negative
        # Positive deltas are more frequent
        delta_sign = random.randint(-3, 5)
        if delta_sign < 0:
            balance_delta = -random.randint(5, 20)
        else:
            balance_delta = random.randint(5, 20)
        data[i]["balance"] += balance_delta
        data[i]["update_timestamp"] = int(time.time())
        events.append({key: value for key, value in data[i].items()})

    # Return the update events
    return events


# Generate new entries to the database
def gen_inserts(data, next_customer_id, size):
    events = []
    for i in range(size):
        ts = int(time.time())
        event = {
            "op": "i",
            "customer_id": next_customer_id + i,
            "balance": random.randint(5, 20),
            "create_timestamp": ts,
            "update_timestamp": ts,
        }
        events.append(event)
    data.extend(events)

    # Return the events for new data
    return events


# Write changes into a csv file, in the "./input_data/" folder
def write_file(events):
    for event in events:
        print(event)

    columns = [
        "op",
        "customer_id",
        "balance",
        "create_timestamp",
        "update_timestamp",
    ]
    random.shuffle(events)

    # Write the file
    file_path = os.path.join(PATH, f"{int(time.time())}.csv")
    with open(file_path, "w") as file:
        writer = csv.writer(file)
        writer.writerow(columns)
        for row in events:
            writer.writerow([row[column] for column in columns])


def main():
    next_customer_id = 1
    events = []
    data = []
    
    # Para  funcionamento da aplicação, o trecho de código foi alterado para não excluir a pasta input_data, uma vez que existe uma thread escutando essa pasta.
    # Assim, apenas os arquivos dentro da pasta são apagados. Além disso, a criação dos arquivos de saída é feita logo a seguir.
    # Recreate the input_data folder
    if os.path.isdir(PATH):
        filelist = [ f for f in os.listdir(PATH)]
        for f in filelist:
            os.remove(os.path.join(PATH, f))
    else:
        os.makedirs(PATH)
    
    #Recreate the output_data folder
    if os.path.isdir(PATH_OUT):
        filelist = [ f for f in os.listdir(PATH_OUT)]
        for f in filelist:
            os.remove(os.path.join(PATH_OUT, f))
        source = pd.DataFrame(columns=['op', 'customer_id', 'balance', 'create_timestamp', 'update_timestamp'])
        target = pd.DataFrame(columns=['op', 'customer_id', 'balance', 'create_timestamp', 'update_timestamp'])
        source.to_csv(os.path.join(PATH_OUT, "updated.csv"), index=False)
        target.to_csv(os.path.join(PATH_OUT, "history.csv"), index=False)
    else:
        os.makedirs(PATH_OUT)

    if len(sys.argv) > 1:
        n_iter = int(sys.argv[1])
    else:
        n_iter = 0

    if n_iter == 0:
        n_iter = int(9E100)

    for _ in range(n_iter):
        # Calculate how many updates and inserts should happen
        n = random.randint(*CHANGES_SIZE)
        # The number of updates should not be larger than the existing data
        updates_number = min([len(data), random.randint(3, n - 1)])
        inserts_number = n - updates_number

        # Generate the updates and inserts changes
        update_events = gen_updates(data, updates_number)
        insert_events = gen_inserts(data, next_customer_id, inserts_number)

        events.extend(update_events)
        events.extend(insert_events)

        if random.randint(0, 4) == 0:
            recent_customers = [
                event["customer_id"]
                for event in update_events + insert_events
            ]
            available_events = list(filter(
                lambda event: event["op"] == "u",
                events
            ))
            available_events = list(filter(
                lambda event: event["customer_id"] not in recent_customers,
                available_events
            ))
            if len(available_events) > 0:
                i = random.randint(0, len(available_events))
                update_events.append(available_events[i])

        # Update the next_customer_id variable to prevent primary key conflict
        next_customer_id += inserts_number

        # Write the changes file
        write_file(update_events + insert_events)

        print("\n")
        time.sleep(SLEEP_TIME)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
