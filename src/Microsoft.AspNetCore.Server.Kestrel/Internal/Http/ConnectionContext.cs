// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Net;
using Microsoft.AspNetCore.Http.Features;

namespace Microsoft.AspNetCore.Server.Kestrel.Internal.Http
{
    using System.IO.Pipelines;

    public class ConnectionContext
    {
        public ConnectionContext()
        {
        }

        public ConnectionContext(ListenerContext context)
        {
            ListenerContext = context;
        }

        public ListenerContext ListenerContext { get; set; }

        public PipelineReaderWriter Input { get; set; }

        public ISocketOutput Output { get; set; }

        public IConnectionControl ConnectionControl { get; set; }

        public IPEndPoint RemoteEndPoint { get; set; }

        public IPEndPoint LocalEndPoint { get; set; }

        public string ConnectionId { get; set; }

        public Action<IFeatureCollection> PrepareRequest { get; set; }
    }
}