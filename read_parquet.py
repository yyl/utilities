import pyarrow.parquet as pq
import polars as pl
import sys
import argparse # Import the argparse library

# Default number of rows (can be overridden by command-line arg)
DEFAULT_NUM_ROWS = 3

def read_parquet_info_polars(file_path, num_rows):
    """
    Reads the schema (using pyarrow) and the first few rows (using polars)
    of a Parquet file.

    Args:
        file_path (str): The path to the Parquet file.
        num_rows (int): The number of rows to read from the beginning.

    Returns:
        None: Prints the schema and data to the console, exits on error.
    """
    try:
        # 1. Read and print the schema using pyarrow
        print("-" * 30)
        print(f"Schema for {file_path} (via pyarrow):")
        print("-" * 30)
        try:
            # Open the Parquet file metadata using pyarrow
            parquet_file = pq.ParquetFile(file_path)
            schema = parquet_file.schema_arrow
            print(schema)
        except Exception as e_schema:
             print(f"Could not read schema using pyarrow: {e_schema}")
             print("Will proceed to try reading data with Polars.")
        print("-" * 30)
        print("\n") # Add some space

        # 2. Read the first N rows using Polars
        print("-" * 30)
        print(f"First {num_rows} rows from {file_path} (via Polars):")
        print("-" * 30)

        # Polars' read_parquet has a convenient 'n_rows' argument
        df_first_rows = pl.read_parquet(file_path, n_rows=num_rows)

        # Print the Polars DataFrame
        print(df_first_rows)

        # Check if the dataframe is empty
        if df_first_rows.height == 0 and num_rows > 0:
             try:
                 metadata = pq.read_metadata(file_path)
                 if metadata.num_rows == 0:
                     print("\nNote: The Parquet file appears to contain 0 rows.")
                 else:
                      # This case could happen if n_rows=0 was passed,
                      # or if there was an issue reading rows despite metadata saying they exist.
                     print(f"\nNote: Read {df_first_rows.height} rows.")
             except Exception:
                 print("\nNote: Read 0 rows (unable to check file metadata for total rows).")


        print("-" * 30)

    except FileNotFoundError:
        # This error is less likely to be caught here now if argparse handles
        # the initial check, but good practice to keep if the file disappears
        # between parsing and opening. Polars might also raise its own IO errors.
        print(f"Error: File not found at {file_path}", file=sys.stderr)
        sys.exit(1)
    except pl.exceptions.ComputeError as e:
         # Catch specific Polars compute errors, often related to reading/parsing
        print(f"Polars Compute Error: {e}", file=sys.stderr)
        sys.exit(1)
    except pl.exceptions.NoDataError:
        # Polars raises this if the file/glob path results in no data
        print(f"Error: Polars reported no data found for path {file_path}", file=sys.stderr)
        sys.exit(1)
    except ImportError:
        print("Error: Make sure 'polars' and 'pyarrow' libraries are installed.", file=sys.stderr)
        print("Run: pip install polars pyarrow", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        # import traceback # Uncomment for detailed debugging
        # traceback.print_exc()
        sys.exit(1)

# --- Main execution block ---
if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(
        description="Read schema and first N rows from a Parquet file using Polars and PyArrow.",
        # Use ArgumentDefaultsHelpFormatter to show default values in help message
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    # Required positional argument for the file path
    parser.add_argument(
        "file_path",
        #type=str is default, but can be explicit
        help="Path to the input Parquet file."
    )

    # Optional argument for the number of rows
    parser.add_argument(
        "-n", "--num_rows",
        type=int,
        default=DEFAULT_NUM_ROWS, # Use the default value defined earlier
        help="Number of rows to read from the beginning."
    )

    # Parse the command-line arguments passed to the script
    args = parser.parse_args()

    # Call the main function with the parsed arguments
    # args.file_path will contain the path provided by the user
    # args.num_rows will contain the number provided, or the default (3)
    read_parquet_info_polars(args.file_path, args.num_rows)