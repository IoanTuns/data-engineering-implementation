from .spark_context import get_spark_session


def main():
    _spark = get_spark_session("DataDP_Main_App")


if __name__ == "__main__":
    main()
