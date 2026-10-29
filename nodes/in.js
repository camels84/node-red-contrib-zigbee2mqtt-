// Node-RED system logger (4 levels); the level is the one chosen in the server config node.
// (v3.1.0: the "in" node now has its own logger, so debug messages of the filter / Home Assistant
//  logic are written with node.logger.debug() and follow the "Log Node Red" level of the server node)
const Z2MLogger = require('../resources/js/node-red-contrib-zigbee2mqtt-logger.js').Logger;

/** Proteção de memória: máximo de mensagens em fila enquanto o estado "enabled" do Home Assistant não é conhecido */
const HA_PENDING_MAX_MESSAGES = 20;

module.exports = function(RED) {
    class Zigbee2mqttNodeIn {
        constructor(config) {
            RED.nodes.createNode(this, config);

            let node = this;
            node.config = config;
            node.cleanTimer = null;
            node.server = RED.nodes.getNode(node.config.server);
            node.last_value = null;
            // v3.1.0: último valor (serializado) das propriedades escolhidas em "filterProperties".
            // O servidor compara-o em nodeSendSingle / nodeSendMultiple.
            node.last_filter_value = undefined;
            node.last_successful_status = {};
            node.status({});

            // Logger Node-RED: segue o nível do nó servidor selecionado (live, sem redeploy)
            node.logger = Z2MLogger.create(node, {
                prefix: 'In',
                getLevel: function () { return node.server ? node.server.debug_level : undefined; }
            });

            // ── v3.1.0: normalizar os novos campos da configuração ──────────────────────────
            // filterProperties: array de chaves do JSON (ou null). Aceita também uma string legada.
            node.config.filterProperties = node.normalizeFilterProperties(node.config.filterProperties);
            // exposeToHomeAssistant: checkbox (boolean, mas tolera 'true' vindo de flows antigos/importados)
            node.config.exposeToHomeAssistant = (node.config.exposeToHomeAssistant === true || node.config.exposeToHomeAssistant === 'true');

            // ── v3.1.0: ciclo de vida Home Assistant ──────────────────────────────────────
            // Enquanto o estado "enabled" não for conhecido (o HA responde através do tópico retained
            // do switch), as mensagens ficam em fila. Quando o servidor chama setHomeAssistantState()
            // a fila é reproduzida (ON) ou descartada (OFF).
            // Sem exposeToHomeAssistant o nó comporta-se exatamente como antes (inicializado + ON).
            node.homeAssistantEnabled = true;
            node.homeAssistantStateInitialized = !node.config.exposeToHomeAssistant;
            node.homeAssistantPendingMessages = [];

            if (node.server) {
                node.listener_onMQTTAvailability = function(data) { node.onMQTTAvailability(data); }
                node.server.on('onMQTTAvailability', node.listener_onMQTTAvailability);

                node.listener_onConnectError = function(data) { node.onConnectError(); }
                node.server.on('onConnectError', node.listener_onConnectError);

                node.listener_onMQTTMessage = function(data) { node.onMQTTMessage(data); }
                node.server.on('onMQTTMessage', node.listener_onMQTTMessage);

                node.listener_onMQTTBridgeState = function(data) { node.onMQTTBridgeState(data); }
                node.server.on('onMQTTBridgeState', node.listener_onMQTTBridgeState);

                // 'removed' (true quando o nó é apagado) é necessário para limpar o discovery retained no HA
                node.on('close', (removed, done) => { node.onClose(removed); done(); });

                // v3.1.0: registar no servidor para o anúncio (MQTT Discovery) e para o estado enabled/disabled
                if (node.config.exposeToHomeAssistant && typeof node.server.registerHomeAssistantNode === 'function') {
                    node.logger.debug('[Z2M] Registering node in the server for Home Assistant exposure');
                    node.server.registerHomeAssistantNode(node);
                }

            } else {
                node.status({
                    fill: "red",
                    shape: "dot",
                    text: "@evilbunny/node-red-contrib-zigbee2mqtt/in:status.no_server"
                });
            }
        }

        /**
         * v3.1.0: converte o valor guardado em filterProperties para um array limpo de strings.
         *   null / undefined / '' / '0'  -> null (sem filtro)
         *   'temperature'                -> ['temperature']
         *   ['a', ' b ', '', 'a']        -> ['a', 'b']
         */
        normalizeFilterProperties(value) {
            if (typeof value === 'string') {
                value = (value && value !== '0') ? [value] : [];
            }
            if (!Array.isArray(value)) {
                return null;
            }
            const clean = [];
            value.forEach(function (prop) {
                if (typeof prop !== 'string') return;
                const trimmed = prop.trim();
                if (trimmed && clean.indexOf(trimmed) < 0) clean.push(trimmed);
            });
            return clean.length ? clean : null;
        }

        onMQTTAvailability(data) {
            let node = this;
            // ROBUSTEZ: Verificar se node.server existe antes de chamar métodos
            if (!node.server) 
                return;
            
            if (data.item && 'ieee_address' in data.item && data.item.ieee_address === node.config.device_id) {
                node.server.nodeSend(node, {
                    'node_send': false
                });
            }
        }

        onMQTTMessage(data) {
            let node = this;

            // ── v3.1.0: Home Assistant (só se o nó estiver exposto) ────────────────────────
            if (node.config.exposeToHomeAssistant) {
                // 1) Estado enabled/disabled ainda desconhecido: guardar para reproduzir depois
                if (!node.homeAssistantStateInitialized) {
                    node.homeAssistantPendingMessages.push(data);
                    while (node.homeAssistantPendingMessages.length > HA_PENDING_MAX_MESSAGES) {
                        node.homeAssistantPendingMessages.shift(); // proteção de memória: descarta a mais antiga
                    }
                    node.logger.debug('[Z2M] Message queued: waiting for the Home Assistant enabled state (' + node.homeAssistantPendingMessages.length + ' pending)');
                    return;
                }
                // 2) Nó desativado através do Home Assistant: descartar
                if (!node.homeAssistantEnabled) {
                    node.logger.debug('[Z2M] Message dropped: node is disabled via Home Assistant');
                    return;
                }
            }

            // v3.3.9: payload MQTT BRUTO (só as chaves publicadas nesta mensagem) para a filtragem granular
            const rawPayload = (data && data.payload && typeof data.payload === 'object') ? data.payload : null;
            node.logger.debug('[Z2M] Granular filtering: raw payload keys [' + (rawPayload ? Object.keys(rawPayload).join(', ') : '-') + ']');

            if (node.config.enableMultiple) {
                if (data.item &&
                    (("ieee_address" in data.item && (node.config.device_id).includes(data.item.ieee_address))
                        ||  ("id" in data.item && (node.config.device_id).includes(data.item.id)))
                ) {
                    node.server.nodeSend(node, {
                        'changed' : data,
                        'retained': data.retained,
                        'filterProperties': node.config.filterProperties,
                        'rawPayload': rawPayload
                    });
                }


            } else {
                if (data.item &&
                    (("ieee_address" in data.item && data.item.ieee_address === node.config.device_id)
                        ||  ("id" in data.item && String(data.item.id) === String(node.config.device_id)))
                ) {
                    node.server.nodeSend(node, {
                        'filter':  node.config.filterChanges,
                        'retained': data.retained,
                        'filterProperties': node.config.filterProperties,
                        'rawPayload': rawPayload
                    });
                }
            }

        }

        /**
         * v3.1.0: chamado pelo servidor quando o estado enabled/disabled do Home Assistant é conhecido
         * (mensagem retained do switch, comando do HA, ou fallback ON após o timeout).
         */
        setHomeAssistantState(enabled) {
            let node = this;
            const wasInitialized = node.homeAssistantStateInitialized;
            const wasEnabled = node.homeAssistantEnabled;

            node.homeAssistantEnabled = !!enabled;
            node.homeAssistantStateInitialized = true;

            if (!wasInitialized || wasEnabled !== node.homeAssistantEnabled) {
                node.logger.notice('Home Assistant state: node is ' + (node.homeAssistantEnabled ? 'ENABLED' : 'DISABLED'));
            }

            const pending = node.homeAssistantPendingMessages;
            node.homeAssistantPendingMessages = [];

            if (node.homeAssistantEnabled) {
                // repor o último estado conhecido (o "disabled" tinha-o substituído)
                node.status(node.last_successful_status);
                if (pending.length) {
                    node.logger.debug('[Z2M] Replaying ' + pending.length + ' message(s) queued while waiting for the Home Assistant state');
                    pending.forEach(function (msg) { node.onMQTTMessage(msg); });
                }
            } else {
                if (pending.length) {
                    node.logger.debug('[Z2M] Message dropped: node is disabled via Home Assistant (' + pending.length + ' queued message(s) discarded)');
                }
                node.status({
                    fill: "grey",
                    shape: "ring",
                    text: "disabled (Home Assistant)"
                });
            }
        }

        onMQTTBridgeState(data) {
            let node = this;
            if (data.payload) {
                node.status(node.last_successful_status);
            } else {
                node.onConnectError();
            }
        }

        onConnectError() {
            this.status({
                fill: "red",
                shape: "dot",
                text: "@evilbunny/node-red-contrib-zigbee2mqtt/in:status.no_connection"
            });
        }


        onClose(removed) {
            let node = this;

            if (node.server) {
                if (node.listener_onMQTTAvailability) {
                    node.server.removeListener("onMQTTAvailability", node.listener_onMQTTAvailability);
                }
                if (node.listener_onConnectError) {
                    node.server.removeListener("onConnectError", node.listener_onConnectError);
                }
                if (node.listener_onMQTTMessage) {
                    node.server.removeListener("onMQTTMessage", node.listener_onMQTTMessage);
                }
                if (node.listener_onMQTTBridgeState) {
                    node.server.removeListener("onMQTTBridgeState", node.listener_onMQTTBridgeState);
                }
                // v3.1.0: libertar o registo Home Assistant (se removed=true limpa também o discovery retained)
                if (node.config.exposeToHomeAssistant && typeof node.server.unregisterHomeAssistantNode === 'function') {
                    node.server.unregisterHomeAssistantNode(node, !!removed);
                }
            }
            node.homeAssistantPendingMessages = [];
            node.onConnectError();
        }

        setSuccessfulStatus(obj) {
            this.status(obj);
            this.last_successful_status = obj;
        }

    }
    RED.nodes.registerType('zigbee2mqtt-eb-in', Zigbee2mqttNodeIn);
};
