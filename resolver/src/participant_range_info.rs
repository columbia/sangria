use common::{full_range_id::FullRangeId, keyspace_id::KeyspaceId};
use proto::resolver::{ParticipantRangeInfo as ProtoParticipantRangeInfo, RangeId as ProtoRangeId};
use std::str::FromStr;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct ParticipantRangeInfo {
    pub participant_range: FullRangeId,
    pub has_writes: bool,
}

impl ParticipantRangeInfo {
    pub fn new(participant_range: FullRangeId, has_writes: bool) -> Self {
        ParticipantRangeInfo {
            participant_range,
            has_writes,
        }
    }
}

impl From<ParticipantRangeInfo> for ProtoParticipantRangeInfo {
    fn from(info: ParticipantRangeInfo) -> Self {
        ProtoParticipantRangeInfo {
            range_id: Some(ProtoRangeId {
                keyspace_id: info.participant_range.keyspace_id.id.to_string(),
                range_id: info.participant_range.range_id.to_string(),
            }),
            has_writes: info.has_writes,
        }
    }
}

impl From<ProtoParticipantRangeInfo> for ParticipantRangeInfo {
    fn from(info: ProtoParticipantRangeInfo) -> Self {
        ParticipantRangeInfo {
            participant_range: FullRangeId {
                keyspace_id: KeyspaceId::from_str(&info.range_id.as_ref().unwrap().keyspace_id)
                    .unwrap(),
                range_id: Uuid::from_str(&info.range_id.as_ref().unwrap().range_id).unwrap(),
            },
            has_writes: info.has_writes,
        }
    }
}
