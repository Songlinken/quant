import dash_html_components as html


def make_html_table(data_set):
    """Return a dash definition of an HTML table for a Pandas dataframe."""
    table = []
    for index_, row in data_set.iterrows():
        html_row = []
        for i in range(len(row)):
            html_row.append(html.Td([row[i]]))
        table.append(html.Tr(html_row))
    return table
