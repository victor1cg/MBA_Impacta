from flask import Flask, request, jsonify
from pipeline import api_pipeline
from predict import predict
from assets.config.config_ingestion import metadados


app = Flask(__name__)

@app.route('/')
def index():
    return "Predição de tempo de voo" 

@app.route('/predict', methods=['get'])
def model():
    df = api_pipeline(
        metadados["api_ingestion"],
        "dev",
        origem = request.args.get('origem'),
        destino = request.args.get('destino')
    )
    return jsonify({'tempo de voo:' : predict(df)})

if __name__ == '__main__':
	app.run(debug=True)
