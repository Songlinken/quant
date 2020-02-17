import dash

app = dash.Dash()
app.scripts.config.serve_locally = True
app.config.suppress_callback_exceptions = True
