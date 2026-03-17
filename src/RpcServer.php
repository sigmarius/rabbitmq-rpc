<?php

declare(strict_types=1);

namespace Rpc;

use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;

/**
 * RPC-сервер: приём запросов из очереди, вызов обработчика по routing_key, отправка ответа в reply_to.
 */
class RpcServer
{
    private AMQPStreamConnection $connection;
    private AMQPChannel $channel;
    private string $queueName;
    /** @var array<string, callable(mixed): string|array|object> обработчики по routing_key */
    private array $handlers = [];
    /** @var callable(string, string, string, string): void|null логирование входящих запросов (routing_key, reply_to, correlation_id, body) */
    private $requestLogger = null;

    public function setRequestLogger(?callable $logger): self
    {
        $this->requestLogger = $logger;
        return $this;
    }

    public function __construct(
        string $host,
        int $port,
        string $user,
        string $password,
        string $queueName,
        string $vhost = '/'
    ) {
        $this->queueName = $queueName;
        $this->connection = new AMQPStreamConnection($host, $port, $user, $password, $vhost);
        $this->channel = $this->connection->channel();
        $this->channel->queue_declare($queueName, false, true, false, false);
    }

    /**
     * Объявить обменник и привязать очередь (чтобы в неё попадали RPC-запросы).
     * @param string $exchange имя обменника (например x_topic)
     * @param string|string[] $bindingKey один ключ или массив ключей (например "*.rpc")
     * @param string $type тип обменника (direct, topic, fanout)
     */
    public function bindExchange(string $exchange, $bindingKey = '#', string $type = 'topic'): self
    {
        $this->channel->exchange_declare($exchange, $type, false, true, false);
        foreach (is_array($bindingKey) ? $bindingKey : [$bindingKey] as $key) {
            $this->channel->queue_bind($this->queueName, $exchange, $key);
        }
        return $this;
    }

    /**
     * Регистрация обработчика для routing_key.
     * @param callable(mixed): string|array|object $handler принимает тело запроса (массив после json_decode или строку), возвращает ответ (будет JSON-сериализован)
     */
    public function handle(string $routingKey, callable $handler): self
    {
        $this->handlers[$routingKey] = $handler;
        return $this;
    }

    /** Запуск цикла обработки сообщений. */
    public function run(): void
    {
        $this->channel->basic_qos(0, 10, false);
        $this->channel->basic_consume(
            $this->queueName,
            '',
            false,
            false,
            false,
            false,
            [$this, 'onMessage']
        );

        while ($this->channel->is_consuming()) {
            $this->channel->wait();
        }
    }

    public function onMessage(AMQPMessage $msg): void
    {
        $replyTo = $msg->has('reply_to') ? $msg->get('reply_to') : null;
        $correlationId = $msg->has('correlation_id') ? $msg->get('correlation_id') : '';
        $body = $msg->getBody();

        $routingKey = '';
        $payload = $body;
        $trimmed = trim($body);
        if ($trimmed !== '' && ($trimmed[0] === '{' || $trimmed[0] === '[')) {
            $decoded = json_decode($body, true);
            if (json_last_error() === JSON_ERROR_NONE && is_array($decoded)) {
                if (isset($decoded['_rpc_routing_key']) && (string) $decoded['_rpc_routing_key'] !== '') {
                    $routingKey = (string) $decoded['_rpc_routing_key'];
                }
                if (array_key_exists('_rpc_payload', $decoded)) {
                    $payload = $decoded['_rpc_payload'];
                } else {
                    $payload = $decoded;
                    unset($payload['_rpc_routing_key']);
                }
            }
        }
        if ($routingKey === '' && $msg->has('application_headers')) {
            $headers = $msg->get('application_headers');
            $table = is_array($headers) ? $headers : (method_exists($headers, 'getNativeData') ? $headers->getNativeData() : []);
            if (is_array($table) && isset($table['x_routing_key']) && (string) $table['x_routing_key'] !== '') {
                $routingKey = (string) $table['x_routing_key'];
            }
        }
        if ($routingKey === '') {
            $routingKey = $msg->getRoutingKey() ?? '';
        }
        if ($routingKey === '' && $msg->has('routing_key')) {
            $routingKey = (string) $msg->get('routing_key');
        }

        if ($this->requestLogger !== null) {
            ($this->requestLogger)($routingKey, (string) $replyTo, (string) $correlationId, $body);
        }

        $responseBody = is_string($payload) ? $payload : json_encode($payload);
        if (isset($this->handlers[$routingKey])) {
            try {
                $result = ($this->handlers[$routingKey])($payload);
                $responseBody = is_string($result) ? $result : json_encode($result);
            } catch (\Throwable $e) {
                $responseBody = json_encode(['error' => $e->getMessage()]);
            }
        }

        if ($replyTo !== null && $replyTo !== '') {
            $this->channel->basic_publish(
                new AMQPMessage($responseBody, [
                    'correlation_id' => $correlationId ?? '',
                ]),
                '',
                $replyTo
            );
        }

        $this->channel->basic_ack($msg->getDeliveryTag());
    }

    public function close(): void
    {
        $this->channel->close();
        $this->connection->close();
    }
}
