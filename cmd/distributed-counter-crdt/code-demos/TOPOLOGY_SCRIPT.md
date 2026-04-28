What you're looking at is a distributed counter that spans seven NATS nodes arranged in a three-level hierarchy — and every increment you send is guaranteed to converge to the same total, no matter which node receives it.

Let's walk through the shape of this thing.

At the top sits Global — the root hub. It has three direct children: America, Asia, and Europe. Each one connects directly to Global.

America and Asia are simple regional nodes. Each one owns a single counter stream, accepts increments from its UI, and publishes them locally. Those writes flow up to Global automatically over the leaf connection.

Europe is different. It's also a leaf node connected to Global, but it's simultaneously a hub for its own sub-regions — Spain, France, and England. Those three connect to Europe the same way Europe connects to Global: via the NATS leaf protocol.

Europe sources the streams from Spain, France, and England, remapping all three into a single regional aggregate. Global then sources Europe, America, and Asia, rolling everything up into one global total. Hit the "Show Breakdown" button and you'll see all six regions tracked independently in real time.

The browser updates are driven by Datastar over Server-Sent Events. Every time a new message lands in a stream, the server pushes a counter update straight to the page. No websockets, no polling.

The punchline: you can hammer any node, disconnect it, reconnect it, and the global total will always converge. That's CRDTs — by design, there's nothing to conflict.
