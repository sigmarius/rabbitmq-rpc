<?php

declare(strict_types=1);

namespace Rpc;

use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Wire\AMQPTable;
/**
 * RPC-клиент: отправка запросов и получение ответов через RabbitMQ.
 * Поддерживает синхронный вызов (call) и асинхронный batch (callBatch).
 */
class RpcClient
{
    private AMQPStreamConnection $connection;
    private AMQPChannel $channel;
    private string $replyQueueName = '';
    private string $consumerTag = '';
    /** @var array<string, array{body: string, resolved: bool}> ответы по correlation_id */
    private array $pending = [];
    private float $timeoutSeconds = 30.0;

    public function __construct(
        string $host = 'localhost',
        int $port = 5672,
        string $user = 'guest',
        string $password = 'guest',
        string $vhost = '/',
        float $readWriteTimeout = 30.0
    ) {
        $this->connection = new AMQPStreamConnection(
            $host,
            $port,
            $user,
            $password,
            $vhost,
            false,
            'AMQPLAIN',
            null,
            'en_US',
            3.0,
            $readWriteTimeout
        );
        $this->channel = $this->connection->channel();
        $this->declareReplyQueue();
    }

    /** Создаём эксклюзивную очередь для ответов (reply_to). */
    private function declareReplyQueue(): void
    {
        $result = $this->channel->queue_declare('', false, false, true, true);
        $this->replyQueueName = $result[0];
        $this->consumerTag = 'rpc_client_' . bin2hex(random_bytes(4));
        $this->channel->basic_consume(
            $this->replyQueueName,
            $this->consumerTag,
            false,
            true,
            false,
            false,
            [$this, 'onReply']
        );
    }

    /** Callback при получении ответа: сохраняем по correlation_id. */
    public function onReply(AMQPMessage $msg): void
    {
        $cid = $msg->get('correlation_id');
        if ($cid === null || $cid === '') {
            return;
        }
        if (isset($this->pending[$cid])) {
            $this->pending[$cid]['body'] = $msg->getBody();
            $this->pending[$cid]['resolved'] = true;
        }
    }

    public function setTimeout(float $seconds): self
    {
        $this->timeoutSeconds = $seconds;
        return $this;
    }

    /**
     * Синхронный RPC: один запрос — один ответ.
     * @param mixed $payload данные запроса (будут JSON-сериализованы)
     * @return string тело ответа (JSON или строка)
     */
    public function call(string $exchange, string $routingKey, $payload): string
    {
        $results = $this->callBatch($exchange, [
            '_single' => ['routing_key' => $routingKey, 'body' => $payload],
        ]);
        return $results['_single'] ?? '';
    }

    /**
     * Пакетный RPC: несколько запросов за раз, ответы в любом порядке.
     * Ключи массива — ID запроса (любой уникальный строковый идентификатор),
     * внутри каждого элемента задаётся routing_key и тело запроса.
     *
     * Пример:
     * [
     *   'req1' => ['routing_key' => 'getUsers.rpc',     'body' => ['login' => 'john13']],
     *   'req2' => ['routing_key' => 'user.getById.rpc', 'body' => ['user_id' => 0]],
     * ]
     *
     * @param array<string, array{routing_key: string, body: mixed}> $requests
     * @return array<string, string> ассоциативный массив ID запроса → тело ответа
     */
    public function callBatch(string $exchange, array $requests): array
    {
        $this->pending = [];
        /** @var array<string, string> $corrIdToRequestId */
        $corrIdToRequestId = [];

        foreach ($requests as $requestId => $request) {
            $routingKey = $request['routing_key'] ?? '';
            $payload    = $request['body'] ?? null;

            $corrId = $this->generateCorrelationId();
            $corrIdToRequestId[$corrId] = (string) $requestId;

            $body = $this->encodeBodyWithRoutingKey($routingKey, $payload);
            $this->pending[$corrId] = ['body' => '', 'resolved' => false];

            $this->channel->basic_publish(
                new AMQPMessage($body, [
                    'correlation_id'     => $corrId,
                    'reply_to'           => $this->replyQueueName,
                    'application_headers'=> new AMQPTable(['x_routing_key' => $routingKey]),
                ]),
                $exchange,
                $routingKey
            );
        }

        $this->waitForPending(array_keys($corrIdToRequestId));

        $result = [];
        foreach ($corrIdToRequestId as $corrId => $requestId) {
            $result[$requestId] = $this->pending[$corrId]['body'] ?? '';
        }
        return $result;
    }

    /** Тело сообщения с зарезервированным полем _rpc_routing_key, чтобы сервер всегда мог определить обработчик. */
    private function encodeBodyWithRoutingKey(string $routingKey, $payload): string
    {
        if (is_string($payload)) {
            return json_encode(['_rpc_routing_key' => $routingKey, '_rpc_payload' => $payload]);
        }
        $data = is_array($payload) ? $payload : (array) $payload;
        $data['_rpc_routing_key'] = $routingKey;
        return json_encode($data);
    }

    private function generateCorrelationId(): string
    {
        return bin2hex(random_bytes(16));
    }

    /** Ждём ответы по списку correlation_id до таймаута. Таймаут wait() ловим, чтобы не падать и вернуть уже пришедшие ответы. */
    private function waitForPending(array $correlationIds): void
    {
        $deadline = microtime(true) + $this->timeoutSeconds;
        while (microtime(true) < $deadline) {
            $allResolved = true;
            foreach ($correlationIds as $cid) {
                if (isset($this->pending[$cid]) && $this->pending[$cid]['resolved']) {
                    continue;
                }
                $allResolved = false;
                break;
            }
            if ($allResolved) {
                break;
            }
            $remaining = max(0.1, $deadline - microtime(true));
            $waitSec = min(2.0, $remaining);
            try {
                $this->channel->wait(null, false, $waitSec);
            } catch (AMQPTimeoutException $e) {
                // таймаут этой итерации — продолжаем цикл до общего дедлайна
            }
        }
    }

    public function close(): void
    {
        $this->channel->close();
        $this->connection->close();
    }
}
