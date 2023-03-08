import re

from .search_report import SearchReport
from .make_logger import make_log


class FormulaModule:
    """
    - report param: A dictionary containing url value to the codal.ir report whether cash flow, balance sheet, profit
     and loss or etc.
    - data_type param: A string with one of these values: balance_sheet, profit_loss or cash_flow
    - data param: A list of dictionaries which each one has some data about a specific cell
    - expr param: A formula expression in string format
    - keys_with_ads and keys_for_ads params: Are used in find_value function to get the value of a specific cell
    - result param: A dict to return the output
    - logger param: an object to log data
    - cell_addr param: a string containing cell address
    - calc_math_value function: It calculates a mathematical expression inside parentheses
    - calc_formula function: It finally calculates the result with the help of the calc_parenthed_value function and
    returns the result
    """

    def __init__(self, report: dict, data_type: str, data: list, expr: str, logger, cell_addr: str,
                 year3_symbol: str = None, result: dict = None, keys_with_ads: dict = None, keys_for_ads: list = None):
        self.expr = expr
        self.data = data
        self.report_url = self.create_report_url(report, data_type)
        self.result = result
        self.keys_with_ads = keys_with_ads
        self.keys_for_ads = keys_for_ads
        self.logger = logger
        self.cell_addr = cell_addr
        self.year3_symbol = year3_symbol

    @staticmethod
    def create_report_url(report: dict, data_type: str):
        url = report.get('Url')
        sheet_ids = {
            "balance_sheet": 0,
            "profit_loss": 1,
            "cash_flow": 9,
            "summ_invest": 3,
            "incoming_profit": 17,
            "out_comp": 5,
            "accepted_comp": 4,
            "accepted_transactions": 6,
            "transfer_transactions": 7
        }

        return f"https://www.stockcodal.com{url}&sheetId={sheet_ids[data_type]}"

    async def get_formula(self):
        expr = self.expr
        if expr[0] == "=":
            expr = expr[1:]
        cells = []
        if expr[:3] == 'SUM':
            formula = expr.split('(')[1].split(')')[0]
            cells.append("SUM")
            if ":" in formula:
                st = formula.split(':')[0]
                fi = formula.split(':')[1]
                # SAME COL SUM
                if st[0] == fi[0]:
                    for i in range(int(st[1:]), int(fi[1:]) + 1):
                        cells.append(st[0] + str(i))
                else:
                    return ''
            elif "," in formula:
                for cell in formula.split(','):
                    cells.append(cell)
            else:
                await make_log(f"New type of formula for cell: {self.cell_addr}: {expr} at report: {self.report_url}",
                               url=self.report_url)
                self.logger.warning(
                    f"New type of formula for cell: {self.cell_addr}: {expr} at report: {self.report_url}")
            return cells

        if expr[:2] == 'IF':
            try:
                # sample = "IF({cell1}<>0,TRUNC({cell2}*{number1}/{cell3},{zero}),{zero})"
                condition = expr.split(",")[0].split("(")[1]  # {cell1}<>0
                if "<>" in condition:
                    cell = condition.split("<>")[0]
                    value = condition.split("<>")[1]
                    if cell != self.cell_addr:
                        cell = await SearchReport.find_value_by_address(self.data, cell)
                    if cell != value:
                        statement = (",".join(expr.split(",")[1:])).split(")")[
                            0]  # TRUNC({cell2}*{number1}/{cell3},{zero}),{zero}
                        if "TRUNC" in statement:
                            math_stmnt = statement.split(",")[0].split("TRUNC(")[1]  # {cell2}*{number1}/{cell3}
                            trunc_num = statement.split(")")[0].split(",")[-1]
                            cells = ["TRUNC", math_stmnt, trunc_num.strip()]
                            return cells
            except Exception as e:
                await make_log(
                    f"Exited while calculating formula for cell: {self.cell_addr}: {expr} at report: {self.report_url}",
                    url=self.report_url)
                self.logger.error(
                    f"Exited while calculating formula for cell: {self.cell_addr}: {expr} at report: {self.report_url}"
                    f" error: {e}")

        '''canonical = 'IF({x}*{y}>0,ROUND(({x}-{y})*100/{y},0),"--")'
        formula = expr.split('>')[0].split('(')[1]  # formula = {x}*{y}
        st = formula.split('*')[0]  # {x}
        fi = formula.split('*')[1]  # {y}
        if canonical.format(x=st, y=fi) != expr:
            # NEED TO ADD LOGGING HERE
            # to know about new formula
            return ''
        cells = ['IF', st, fi]
        return cells'''

        if expr[:5] == "ROUND":
            value = expr[5:].split(",")[0]
            if value[0] == "(":
                value = value[1:]
            else:
                await make_log(f"new type of formula for cell: {self.cell_addr}: {expr} at report: {self.report_url}",
                               url=self.report_url)
                self.logger.error(
                    f"new type of formula for cell: {self.cell_addr}: {expr} at report: {self.report_url}")

            digit_num = expr[5:].split(",")[1].replace(")", "")
            cells = ['ROUND', value, digit_num]
            return cells
        if "+" in expr:
            # Formula type: B1+B2+B3+...
            cells = ['SUM', *(expr.split("+"))]
            return cells
        else:
            try:
                cells = ["MINUS", expr]
                return cells
            except Exception as e:
                await make_log(
                    f"Exited while calculating formula for cell: {self.cell_addr}: {expr} at report: {self.report_url}",
                    url=self.report_url)
                self.logger.error(
                    f"Exited while calculating formula for cell: {self.cell_addr}: {expr} at report: {self.report_url}")

    async def calc_form1_math_value(self, value: str) -> float:  # (B22*A48)/B47
        expr1 = re.findall(r"[A-Z][1-9][0-9]*[*][A-Z][1-9][0-9]*", value)[0]
        # expr1 = value.split("/")[0].replace("(", "").replace(")", "")  # B22*A48
        expr2 = re.findall(r"[/][A-Z][1-9][0-9]*", value)[0]
        # expr2 = "/" + value.split("/")[1].replace("(", "").replace(")", "")  # /B47

        result = float(await SearchReport.find_value_by_address(self.data, expr1.split("*")[0], self.year3_symbol,
                                                                self.keys_with_ads, self.keys_for_ads,
                                                                self.result)) * float(
            await SearchReport.find_value_by_address(self.data, expr1.split("*")[1], self.year3_symbol,
                                                     self.keys_with_ads, self.keys_for_ads, self.result))
        try:
            result /= float(
                (await SearchReport.find_value_by_address(self.data, expr2[1:], self.year3_symbol,
                                                          self.keys_with_ads, self.keys_for_ads, self.result)))
        except ZeroDivisionError:
            result = float(0)

        return result

    async def calc_form2_math_value(self, value: str) -> float:  # (B21/B29)*A30
        expr1 = re.findall(r"[A-Z][1-9][0-9]*[/][A-Z][1-9][0-9]*", value)[0]
        expr2 = re.findall(r"[*][A-Z][1-9][0-9]*", value)[0]

        try:
            result = float(await SearchReport.find_value_by_address(self.data, expr1.split("/")[0], self.year3_symbol,
                                                                    self.keys_with_ads, self.keys_for_ads,
                                                                    self.result)) / float(
                await SearchReport.find_value_by_address(self.data, expr1.split("/")[1], self.year3_symbol,
                                                         self.keys_with_ads, self.keys_for_ads, self.result))
        except ZeroDivisionError:
            result = float(0)

        result *= float(
            (await SearchReport.find_value_by_address(self.data, expr2[1:], self.year3_symbol,
                                                      self.keys_with_ads, self.keys_for_ads, self.result)))

        return result

    async def calc_math_value(self, value: str, math_type: str = None) -> float:
        """
        form1: (B22*A48)/B47
        form2: (B21/B29)*A30
        """
        if math_type == "minus":
            result = float(await SearchReport.find_value_by_address(self.data, value.split("-")[0])) - float(
                await SearchReport.find_value_by_address(self.data, value.split("-")[1]))
            return result

        form1_matches = re.findall(r"[(][A-Z][1-9][0-9]*[*][A-Z][1-9][0-9]*[)][/][A-Z][1-9][0-9]*", value)
        if len(form1_matches) != 0:
            return await self.calc_form1_math_value(form1_matches[0])

        form2_matches = re.findall(r"[(][A-Z][1-9][0-9]*[/][A-Z][1-9][0-9]*[)][*][A-Z][1-9][0-9]*", value)
        if len(form2_matches) != 0:
            return await self.calc_form2_math_value(form2_matches[0])

        # expr1 = value.split(")")[0].split("(")[1]
        # expr2 = value.split(")")[1]

        await make_log(
            f"New type of expression for cell: {self.cell_addr}: {value} in formula at report: {self.report_url}",
            url=self.report_url)
        self.logger.warning(
            f"New type of expression for cell: {self.cell_addr}: {value} in formula at report: {self.report_url}")

    async def calc_formula(self) -> str:
        formula = await self.get_formula()
        if formula[0] == 'SUM':
            ads = formula[1:]
            result = 0
            for address in ads:
                result += int(
                    await SearchReport.find_value_by_address(self.data, address, self.year3_symbol, self.keys_with_ads,
                                                             self.keys_for_ads, self.result))

            return str(int(result))
        elif formula[0] == 'ROUND':
            result = await self.calc_math_value(formula[1])
            result = round(result, int(formula[2]))
            return str(int(result))
        elif formula[0] == "TRUNC":
            result = await self.calc_math_value(formula[1])
            if formula[2] == '0':
                return str(int(result))
            else:
                await make_log(f"New trunc number: {formula[2]}", url=self.report_url)
                self.logger.error(f"New trunc number: {formula[2]} at report: {self.report_url}")
        elif formula[0] == "MINUS":
            result = await self.calc_math_value(formula[1], math_type="minus")
            return str(int(result))
