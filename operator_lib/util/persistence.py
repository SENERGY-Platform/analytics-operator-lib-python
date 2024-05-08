import os 
import pickle 

def load(data_path, file_name, default=None):
    file_path = os.path.join(data_path, file_name)
    if not os.path.exists(file_path):
        return default 
    with open(file_path, 'rb') as f:
        timestamp = pickle.load(f)
        return timestamp

def save(data_path, file_name, value):
    if not os.path.exists(data_path):
        os.makedirs(data_path)
    file_path = os.path.join(data_path, file_name)
    with open(file_path, 'wb') as f:
        pickle.dump(value, f)