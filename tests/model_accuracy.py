import sys
import os

# Add the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.db.model_db_util import get_accuracy

def main():
    event = 'mergers_acquisitions'
    accuracy = get_accuracy(event)
    
    if accuracy is not None:
        print(f"The accuracy for '{event}' is: {accuracy:.2f}")
    else:
        print(f"No accuracy data found for '{event}'")

if __name__ == "__main__":
    main()
