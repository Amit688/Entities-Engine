package org.z.entities.engine;

import java.util.List;
import java.util.UUID;

import akka.stream.UniqueKillSwitch;

public class StreamDescriptor {
	private UniqueKillSwitch killSwitch;
	private UUID uuid;
	private List<SourceDescriptor> topicDescriptors;
	
	public StreamDescriptor(UniqueKillSwitch killSwitch, UUID uuid, List<SourceDescriptor> topicDescriptors) {
		this.killSwitch = killSwitch;
		this.uuid = uuid;
		this.topicDescriptors = topicDescriptors;
	}

	public UniqueKillSwitch getKillSwitch() {
		return killSwitch;
	}

	public UUID getUuid() {
		return uuid;
	}

	public List<SourceDescriptor> getSourceDescriptors() {
		return topicDescriptors;
	}
}
