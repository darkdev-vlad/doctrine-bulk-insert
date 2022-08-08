<?php
declare(strict_types=1);

namespace Franzose\DoctrineBulkInsert\Tests;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Platforms\PostgreSqlPlatform;
use Franzose\DoctrineBulkInsert\Query;
use PHPUnit\Framework\TestCase;

final class QueryTest extends TestCase
{
    public function testExecute(): void
    {
        $connection = $this->prophesize(Connection::class);

        $connection->getDatabasePlatform()
            ->shouldBeCalledOnce()
            ->willReturn(new PostgreSqlPlatform());

        $connection->executeStatement('INSERT INTO foo (foo, bar) VALUES (?, ?), (?, ?);', [111, 222, 333, 444], [])
            ->shouldBeCalledOnce()
            ->willReturn(2);

        $connection->executeUpdate()
            ->shouldNotBeCalled();

        $rows = (new Query($connection->reveal()))->execute('foo', [
            ['foo' => 111, 'bar' => 222],
            ['foo' => 333, 'bar' => 444],
        ]);

        static::assertEquals(2, $rows);
    }

    public function testExecuteWithIgnore(): void
    {
        $connection = $this->prophesize(Connection::class);

        $connection->getDatabasePlatform()
            ->shouldBeCalledOnce()
            ->willReturn(new PostgreSqlPlatform());

        $connection->executeStatement(
            'INSERT IGNORE INTO foo (foo, bar) VALUES (?, ?), (?, ?), (?, ?);',
            [111, 222, 333, 444, 555, 666],
            []
        )
            ->shouldBeCalledOnce()
            ->willReturn(3);

        $connection->executeUpdate()
            ->shouldNotBeCalled();

        $rows = (new Query($connection->reveal()))->execute(
            'foo',
            [
                ['foo' => 111, 'bar' => 222],
                ['foo' => 333, 'bar' => 444],
                ['foo' => 555, 'bar' => 666],
            ],
            [],
            true
        );

        static::assertEquals(3, $rows);
    }

    public function testExecuteWithEmptyDataset(): void
    {
        $connection = $this->prophesize(Connection::class);

        $connection->getDatabasePlatform()
            ->shouldNotBeCalled();

        $connection->executeUpdate()
            ->shouldNotBeCalled();

        $rows = (new Query($connection->reveal()))->execute('foo', []);

        static::assertEquals(0, $rows);
    }
}
