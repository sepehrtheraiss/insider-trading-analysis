# Prettify y axis: 2000000 to $2M
def millions_formatter(x,y=0):
    return '$ {:,.0f} M'.format(x*1e-6)