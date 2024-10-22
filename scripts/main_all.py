from main_agg import main as gold
from main_ingest import main as bronze
from main_transform import main as silver


def main():
    bronze()
    silver()
    gold()


if __name__ == "__main__":
    main()
