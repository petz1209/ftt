
import argparse
from main import file_importer

def main():
    parser = argparse.ArgumentParser(description="cli tool to dump files into sqlite database")
    parser.add_argument("-s", "--source", type=str, help="Enter the folder path that you want to convert.")
    parser.add_argument("-d", "--destination", type=str, help="either sqlite file name or database connection string.")
    parser.add_argument("-f", "--filter", type=str, help="filter on a specific file type.")
    args = parser.parse_args()

    if not args.source and not args.destination and not args.filter:
        print("Welcome to FtT - Files to Tables")
        print()
        print("In order to use this tool you need to specifiy a source folder and a destination database")
        print("Options:")
        print("-s  / --source:  define that path to the folder you want to dump to db")
        print("-d  / --destination:  define the database file name or connection string you want to dump to")
        print("-f  / --filter:  Optionally filter for file extensions you want to consider (fe. txt, csv, ...)")
        exit(0)

    if not args.source:
        print("No source defined")
        exit(1)
    if not args.destination:
        print("No destination db defined")
        exit(1)

    file_importer(args.source, db_name=args.destination, file_type=args.filter)

if __name__ == '__main__':
    main()




