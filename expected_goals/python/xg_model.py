import os
import sys
import pickle
import logging
import logging.config

import pandas as pd

from sklearn.metrics import mean_squared_error
from sklearn.model_selection import GridSearchCV, train_test_split

from build_X import build_X
from constants_xg import (
    ID_COLS, MODEL, MODEL_FILES_DIR, PROJECT_DIR, TARGET_COL
)

sys.path.append(PROJECT_DIR)

from config import config

logger = logging.getLogger('main_logger')

LOGS_DIR = os.path.join(PROJECT_DIR, 'logs')

os.makedirs(LOGS_DIR, exist_ok=True)
os.chdir(LOGS_DIR)


def get_table(train_table, model_cols):

    for col in model_cols:
        train_table[col] = train_table[col].astype(str)

    x_cols = ID_COLS + model_cols

    X = train_table[x_cols]

    y = train_table[TARGET_COL].values.ravel()

    logger.info("There are {} lines from shots".format(train_table.shape[0]))
    logger.info("There are {} variables".format(X.shape[1]))

    return X, y


def pick_best_model_parameters(model, parameters, X_train, y_train):
    clf = GridSearchCV(model, parameters, cv=4, n_jobs=-1)
    clf.fit(X_train, y_train)

    return clf.best_estimator_


def get_predictions(df, total_games_df, model_cols, clf):
    X, y = get_table(df, model_cols)
    y_pred = clf.predict(X)
    df['xg'] = y_pred
    final_df = pd.merge(
        total_games_df, df[['game_id', 'sequence_number', 'xg']], how='left'
    )
    return final_df


def get_model(df):

    logger.info("Loading data...")
    pred_data_df, train_df, validation_df, model_cols = build_X(df)

    X, y = get_table(train_df, model_cols)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.25, random_state=0
    )

    model_paths = os.path.join(MODEL_FILES_DIR, 'model_data')
    os.makedirs(model_paths, exist_ok=True)
    model_files_dir = list(os.walk(model_paths))[0]
    pickle_path = os.path.join(model_paths, 'forest.pkl')

    if len(model_files_dir) and len(
        [x for x in model_files_dir[2] if x.endswith('pkl')]
    ):
        with open(pickle_path, 'rb') as f:
            clf = pickle.load(f)
        logger.info("Model loaded from {}".format(pickle_path))
    else:

        clf, parameters = MODEL
        logger.info("Selecting the best parameters for the model...")

        clf = pick_best_model_parameters(clf, parameters, X_train, y_train)
        logger.info("Got Model")

        with open(pickle_path, 'wb') as f:
            pickle.dump(clf, f)
            logger.info("File saved to {}".format(pickle_path))

    y_pred = clf.predict(X_test)
    msq = mean_squared_error(y_test, y_pred)

    logger.info("The mean squared error is {}".format(msq))
    msq_path = os.path.join(model_paths, 'mean_squared_error.txt')

    with open(msq_path, 'w') as f:
        f.write(str(msq))
        logger.info("File saved to {}".format(msq_path))

    predicted_df = get_predictions(
        validation_df, pred_data_df, model_cols, clf
    )

    return predicted_df


if __name__ == '__main__':
    logging.config.dictConfig(config['logger'])

    from datetime import datetime

    t0 = datetime.now()
    predicted_df = get_model()
    t1 = datetime.now()
    logger.info("The process took {}".format(t1 - t0))
