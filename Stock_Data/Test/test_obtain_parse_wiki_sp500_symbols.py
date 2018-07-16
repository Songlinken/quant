from Stock_Data.Source.obtain_parse_wiki_sp500_symbols import Obtain_parse_sp500_symbols


def test_obtain_parse_wiki_sp500_symbols():
    model = Obtain_parse_sp500_symbols()
    symbols = model.obtain_wiki_sp500_symbols()

    model.insert_sp500_symbols(symbols)