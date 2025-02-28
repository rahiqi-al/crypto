import happybase
import subprocess
from thrift.transport.TTransport import TTransportException

def load(ds):
    try:
        year, month, day = ds.split('-')

        hdfs_dir = f"/user/etudiant/crypto/processed/YYYY={year}/MM={month}/DD={day}/"
        local_file = "/tmp/crypto_agg.csv"

        # Remove existing file and download from HDFS
        subprocess.run(["rm", "-f", local_file])  # Delete if exists
        result = subprocess.run(["hdfs", "dfs", "-get", hdfs_dir + "part-00000", local_file])
        if result.returncode != 0:
            raise Exception(f"Failed to download file from HDFS: return code {result.returncode}")

        # Connect to HBase
        connection = happybase.Connection('localhost')

        # Check if table exists, create if not
        tables = connection.tables()
        if b'crypto_prices' not in tables:
            connection.create_table('crypto_prices', {'stats': dict()})

        # Access the table
        table = connection.table('crypto_prices')

        # Load data from file
        with open(local_file, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                coin_id, agg_str = line.split("\t")
                min_p, max_p, avg_p, std_p, vol_sum, c = agg_str.split(",")

                row_key = f"{coin_id}_{ds}"
                table.put(row_key, {
                    b'stats:price_min': min_p.encode(),
                    b'stats:price_max': max_p.encode(),
                    b'stats:price_avg': avg_p.encode(),
                    b'stats:price_std_dev': std_p.encode(),
                    b'stats:volume_sum': vol_sum.encode(),
                    b'stats:count': c.encode()
                })

    except subprocess.CalledProcessError as e:
        print(f"HDFS command failed: {e}")
    except TTransportException as e:
        print(f"HBase connection failed: {e}")
    except FileNotFoundError as e:
        print(f"File not found: {e}")
    except ValueError as e:
        print(f"Data parsing error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        try:
            connection.close()
        except:
            pass

