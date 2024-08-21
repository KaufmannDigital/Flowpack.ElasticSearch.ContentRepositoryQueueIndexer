<?php
declare(strict_types=1);

namespace Flowpack\ElasticSearch\ContentRepositoryQueueIndexer\Indexer;

/*
 * This file is part of the Flowpack.ElasticSearch.ContentRepositoryQueueIndexer package.
 *
 * (c) Contributors of the Neos Project - www.neos.io
 *
 * This package is Open Source Software. For the full copyright and license
 * information, please view the LICENSE file which was distributed with this
 * source code.
 */

use Flowpack\ElasticSearch\ContentRepositoryAdaptor;
use Flowpack\ElasticSearch\ContentRepositoryQueueIndexer\Command\NodeIndexQueueCommandController;
use Flowpack\ElasticSearch\ContentRepositoryQueueIndexer\IndexingJob;
use Flowpack\ElasticSearch\ContentRepositoryQueueIndexer\RemovalJob;
use Flowpack\JobQueue\Common\Job\JobManager;
use Neos\ContentRepository\Domain\Model\NodeInterface;
use Neos\Flow\Annotations as Flow;
use Neos\Flow\Persistence\PersistenceManagerInterface;

/**
 * NodeIndexer for use in batch jobs
 *
 * @Flow\Scope("singleton")
 */
class NodeIndexer extends ContentRepositoryAdaptor\Indexer\NodeIndexer
{
    /**
     * @var JobManager
     * @Flow\Inject
     */
    protected $jobManager;

    /**
     * @var PersistenceManagerInterface
     * @Flow\Inject
     */
    protected $persistenceManager;

    /**
     * @var bool
     * @Flow\InjectConfiguration(path="enableLiveAsyncIndexing")
     */
    protected $enableLiveAsyncIndexing;

    /**
     * @Flow\InjectConfiguration(path="batchSize")
     * @var int
     */
    protected $queueBatchSize;

    protected array $nodesToIndex = [];
    protected array $nodesToRemove = [];

    /**
     * @param NodeInterface $node
     * @param string|null $targetWorkspaceName In case indexing is triggered during publishing, a target workspace name will be passed in
     * @throws ContentRepositoryAdaptor\Exception
     */
    public function indexNode(NodeInterface $node, $targetWorkspaceName = null): void
    {
        if ($node->isRemoved()) {
            $this->removeNode($node, $targetWorkspaceName);
            return;
        }
        if ($this->enableLiveAsyncIndexing !== true) {
            parent::indexNode($node, $targetWorkspaceName);

            return;
        }

        if ($this->settings['indexAllWorkspaces'] === false) {
            if ($targetWorkspaceName !== null && $targetWorkspaceName !== 'live') {
                return;
            }

            if ($targetWorkspaceName === null && $node->getContext()->getWorkspaceName() !== 'live') {
                return;
            }
        }

        $this->nodesToIndex[$targetWorkspaceName??$node->getContext()->getWorkspaceName()][$node->getIdentifier()] = $this->nodeAsArray($node)[0];
        $this->flushIfNeeded();
    }

    /**
     * @param NodeInterface $node
     * @param string|null $targetWorkspaceName In case indexing is triggered during publishing, a target workspace name will be passed in
     * @throws ContentRepositoryAdaptor\Exception
     * @throws \Flowpack\ElasticSearch\Exception
     * @throws \Neos\Flow\Persistence\Exception\IllegalObjectTypeException
     * @throws \Neos\Utility\Exception\FilesException
     */
    public function removeNode(NodeInterface $node, string $targetWorkspaceName = null): void
    {
        if ($this->enableLiveAsyncIndexing !== true) {
            parent::removeNode($node, $targetWorkspaceName);
            return;
        }

        if ($this->settings['indexAllWorkspaces'] === false) {
            if ($targetWorkspaceName !== null && $targetWorkspaceName !== 'live') {
                return;
            }

            if ($targetWorkspaceName === null && $node->getContext()->getWorkspaceName() !== 'live') {
                return;
            }
        }

        $this->nodesToRemove[$targetWorkspaceName??$node->getContext()->getWorkspaceName()][$node->getIdentifier()] = $this->nodeAsArray($node)[0];
        $this->flushIfNeeded();
    }

    protected function nodesToIndexLength(): int
    {
        return array_reduce(
            $this->nodesToIndex,
            function ($carry, $nodes) {
                return $carry + sizeof($nodes);
            },
            0
        );
    }

    protected function nodesToRemoveLength(): int
    {
        return array_reduce(
            $this->nodesToRemove,
            function ($carry, $nodes) {
                return $carry + sizeof($nodes);
            },
            0
        );
    }

    protected function bulkRequestLength(): int
    {
        return $this->nodesToRemoveLength() + $this->nodesToIndexLength();
    }

    protected function flushIfNeeded(): void
    {
        if ($this->bulkRequestLength() >= $this->queueBatchSize) {
            $this->flush();
        }
        if ($this->bulkRequestLength() >= $this->batchSize['elements'] || $this->bulkRequestSize() >= $this->batchSize['octets']) {
            parent::flush();
        }
    }

    public function flush(): void
    {
        foreach ($this->nodesToIndex as $targetWorkspaceName => $nodesToIndex) {
            $indexingJob = new IndexingJob($this->indexNamePostfix, $targetWorkspaceName, array_values($nodesToIndex));
            $this->jobManager->queue(NodeIndexQueueCommandController::LIVE_QUEUE_NAME, $indexingJob);
        }

        foreach ($this->nodesToRemove as $targetWorkspaceName => $nodesToRemove) {
            $removalJob = new RemovalJob($this->indexNamePostfix, $targetWorkspaceName, array_values($nodesToRemove));
            $this->jobManager->queue(NodeIndexQueueCommandController::LIVE_QUEUE_NAME, $removalJob);
        }
        parent::flush();
    }

    protected function reset(): void
    {
        $this->nodesToIndex = [];
        $this->nodesToRemove = [];
        parent::reset();
    }

    /**
     * Returns an array of data from the node for use as job payload.
     *
     * @param NodeInterface $node
     * @return array
     */
    protected function nodeAsArray(NodeInterface $node): array
    {
        return [
            [
                'persistenceObjectIdentifier' => $this->persistenceManager->getIdentifierByObject($node->getNodeData()),
                'identifier' => $node->getIdentifier(),
                'dimensions' => $node->getContext()->getDimensions(),
                'workspace' => $node->getWorkspace()->getName(),
                'nodeType' => $node->getNodeType()->getName(),
                'path' => $node->getPath()
            ]
        ];
    }
}
