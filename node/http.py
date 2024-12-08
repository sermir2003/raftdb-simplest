from flask import Flask, request, jsonify, g, redirect


def create_app(raft_node):
    app = Flask(f'node-{id}')

    @app.before_request
    def attach_state():
        g.raft_node = raft_node

    @app.route('/items/<string:key>', methods=['PUT'])
    def create(key):
        try:
            result = g.raft_node.on_change_request({
                'type': 'create',
                'key': key,
                'value': request.get_json()['value'],
            })
            if result['message'] == 'ok':
                return jsonify({'message': 'ok'}), 200
            elif result['message'] == 'redirect':
                return redirect(result['url'])
            else:
                return jsonify(result), 400
        except Exception as e:
            return jsonify({'message': str(e)}), 500

    @app.route('/items/<string:key>', methods=['GET'])
    def read(key):
        try:
            return jsonify({'message': 'ok', 'value': g.raft_node.on_read_request(key)}), 200
        except Exception as e:
            return jsonify({'message': str(e)}), 500

    @app.route('/items/<string:key>', methods=['POST'])
    def update(key):
        try:
            result = g.raft_node.on_change_request({
                'type': 'update',
                'key': key,
                'value': request.get_json()['value'],
            })
            if result['message'] == 'ok':
                return jsonify({'message': 'ok'}), 200
            elif result['message'] == 'redirect':
                return redirect(result['url'])
            else:
                return jsonify(result), 400
        except Exception as e:
            return jsonify({'message': str(e)}), 500

    @app.route('/items/<string:key>', methods=['DELETE'])
    def delete(key):
        try:
            result = g.raft_node.on_change_request({
                'type': 'delete',
                'key': key,
            })
            if result['message'] == 'ok':
                return jsonify({'message': 'ok'}), 200
            elif result['message'] == 'redirect':
                return redirect(result['url'])
            else:
                return jsonify(result), 400
        except Exception as e:
            return jsonify({'message': str(e)}), 500

    @app.route('/items/cas/<string:key>', methods=['POST'])
    def cas(key):
        try:
            data = request.get_json()
            result = g.raft_node.on_change_request({
                'type': 'cas',
                'key': key,
                'expected': data['expected'],
                'desired': data['desired'],
            })
            if result['message'] == 'ok':
                return jsonify({'message': 'ok'}), 200
            elif result['message'] == 'redirect':
                return redirect(result['url'])
            else:
                return jsonify(result), 400
        except Exception as e:
            return jsonify({'message': str(e)}), 500

    @app.route('/raft_control', methods=['POST'])
    def raft_control(key):
        try:
            g.raft_node.manage_control_request(request.get_json())
        except Exception as e:
            return jsonify({'message': str(e)}), 500


    return app
