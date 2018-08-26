import pandas as pd
import numpy as np
import MySQLdb as mdb
import matplotlib.pyplot as plt
from matplotlib.collections import LineCollection
from sqlalchemy import create_engine
from sklearn import covariance, cluster, manifold


# database connection to the MySQL instance
class mysql_engine():
    db_host = 'localhost'
    db_user = 'sguo'
    db_pass = 'gsl1990~'
    db_name = 'stock'
    connection = mdb.connect(db_host, db_user, db_pass, db_name)
    engine = create_engine('mysql+mysqldb://sguo:gsl1990~@localhost:3306/stock?charset=utf8')


def get_data():

    sql = """
             SELECT dp.price_date, dp.open_price, dp.adj_close_price, s.ticker
             FROM daily_price dp
             LEFT JOIN symbol s
             ON dp.symbol_id = s.id
             WHERE dp.price_date
             BETWEEN '2000-01-01' AND '2015-01-01';
          """

    try:
        with mysql_engine().connection:
            data_set = pd.read_sql(sql, con=mysql_engine().connection)

        mysql_engine().connection.close()

    except Exception as e:
        print('Could not get data: %s' % e)

    return data_set


def sp500_cluster(data_set):
    data_set['diff'] = data_set['adj_close_price'] - data_set['open_price']
    data_set = data_set.pivot(index='price_date', columns='ticker', values='diff')
    data_set = data_set.fillna(0)
    data_set_std = data_set / data_set.std(0)

    # choose EmpiricalCovariance to estimate correlation matrix
    edge_model = covariance.EmpiricalCovariance()
    edge_model.fit(data_set_std)

    centers, labels = cluster.affinity_propagation(edge_model.covariance_)

    # print cluster result
    print 'Centers : \n', ', '.join(np.array(data_set_std.columns.tolist())[centers])

    n_labels = labels.max()
    for i in range(n_labels + 1):
        print('Cluster %i: %s' % ((i + 1), ', '.join(np.array(data_set_std.columns.tolist())[labels == i])))

    # visualization
    node_position_model = manifold.LocallyLinearEmbedding(n_components=2, eigen_solver='dense', n_neighbors=6)

    embedding = node_position_model.fit_transform(data_set_std.T).T

    plt.figure(1, facecolor='w', figsize=(10, 8))
    plt.clf()
    ax = plt.axes([0., 0., 1., 1.])
    plt.axis('off')

    # Display a graph of the partial correlations
    partial_correlations = edge_model.precision_.copy()
    d = 1 / np.sqrt(np.diag(partial_correlations))
    partial_correlations *= d
    partial_correlations *= d[:, np.newaxis]
    non_zero = (np.abs(np.triu(partial_correlations, k=1)) > 0.02)

    # Plot the nodes using the coordinates of our embedding
    plt.scatter(embedding[0], embedding[1], s=100 * d ** 2, c=labels,
                cmap=plt.cm.nipy_spectral)

    # Plot the edges
    start_idx, end_idx = np.where(non_zero)
    # a sequence of (*line0*, *line1*, *line2*), where::
    #            linen = (x0, y0), (x1, y1), ... (xm, ym)
    segments = [[embedding[:, start], embedding[:, stop]]
                for start, stop in zip(start_idx, end_idx)]
    values = np.abs(partial_correlations[non_zero])
    lc = LineCollection(segments,
                        zorder=0, cmap=plt.cm.hot_r,
                        norm=plt.Normalize(0, .7 * values.max()))
    lc.set_array(values)
    lc.set_linewidths(15 * values)
    ax.add_collection(lc)

    # Add a label to each node. The challenge here is that we want to
    # position the labels to avoid overlap with other labels
    for index, (name, label, (x, y)) in enumerate(zip(np.array(data_set_std.columns.tolist()), labels, embedding.T)):
        dx = x - embedding[0]
        dx[index] = 1
        dy = y - embedding[1]
        dy[index] = 1
        this_dx = dx[np.argmin(np.abs(dy))]
        this_dy = dy[np.argmin(np.abs(dx))]

        if this_dx > 0:
            horizontalalignment = 'left'
            x = x + .002
        else:
            horizontalalignment = 'right'
            x = x - .002
        if this_dy > 0:
            verticalalignment = 'bottom'
            y = y + .002
        else:
            verticalalignment = 'top'
            y = y - .002

        plt.text(x, y, name, size=10,
                 horizontalalignment=horizontalalignment,
                 verticalalignment=verticalalignment,
                 bbox=dict(facecolor='w',
                           edgecolor=plt.cm.nipy_spectral(label / float(n_labels)),
                           alpha=.6))

    plt.xlim(embedding[0].min() - .15 * embedding[0].ptp(),
             embedding[0].max() + .10 * embedding[0].ptp(),)
    plt.ylim(embedding[1].min() - .03 * embedding[1].ptp(),
             embedding[1].max() + .03 * embedding[1].ptp())

    plt.show()


if __name__ == '__main__':
    data_set = get_data()
    sp500_cluster(data_set)