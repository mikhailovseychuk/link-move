package com.nhl.link.etl.unit;

import static org.junit.Assert.assertEquals;

import com.nhl.link.etl.connect.StreamConnector;
import com.nhl.link.etl.runtime.connect.URIConnectorFactory;
import org.junit.After;
import org.junit.Before;

import com.nhl.link.etl.Execution;
import com.nhl.link.etl.connect.Connector;
import com.nhl.link.etl.runtime.EtlRuntime;
import com.nhl.link.etl.runtime.EtlRuntimeBuilder;
import com.nhl.link.etl.runtime.jdbc.DataSourceConnector;

public abstract class EtlIntegrationTest extends DerbySrcTargetTest {

	protected EtlRuntime etl;

	@Before
	public void before() {
		this.etl = createEtl();
	}

	@After
	public void shutdown() {
		if (etl != null) {
			etl.shutdown();
		}
	}

	protected EtlRuntime createEtl() {
		Connector c = new DataSourceConnector(srcDataSource);
		return new EtlRuntimeBuilder().withConnector("derbysrc", c).withTargetRuntime(targetStack.runtime())
				.withConnectorFactory(StreamConnector.class, new URIConnectorFactory())
				.build();
	}

	protected void assertExec(int extracted, int created, int updated, int deleted, Execution exec) {
		assertEquals("Extracted unexpected number of records.", extracted, exec.getStats().getExtracted());
		assertEquals("Created unexpected number of records.", created, exec.getStats().getCreated());
		assertEquals("Updated unexpected number of records.", updated, exec.getStats().getUpdated());
		assertEquals("Deleted unexpected number of records.", deleted, exec.getStats().getDeleted());

	}
}
