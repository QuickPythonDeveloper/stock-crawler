from decimal import Decimal


class ConvertText:

    @staticmethod
    def convert_billion_number(number: str):
        return Decimal(number) * pow(10, 9)

    @staticmethod
    def is_float(value: str) -> bool:
        try:
            float(value)
            return True
        except Exception:
            return False
