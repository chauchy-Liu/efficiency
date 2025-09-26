import os
import joblib


def load_model(windFarm, algName, assetId, model_name):
    asset_model_path = os.path.join('model',windFarm, algName, assetId, model_name + '.model')
    common_model_path = os.path.join('model',windFarm, algName, model_name + '.model')
    if os.path.exists(asset_model_path):
        return joblib.load(asset_model_path)
    else:
        return joblib.load(common_model_path)
    

if __name__ == '__main__':
    model = load_model('0AxB5ibC', 'speed_power')
    print(model)