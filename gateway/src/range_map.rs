use itertools::Itertools;
use std::collections::BTreeSet;
use std::ops::Bound::Included;
use std::ops::Range as StdRange;

type Range = StdRange<u32>;

#[derive(Debug)]
pub struct RangeMap {
    inner: BTreeSet<u32>,
}

impl RangeMap {
    pub fn from_sorted(nums: &[(u32, u32)]) -> Result<Self, ()> {
        let mut inner = BTreeSet::new();

        inner.insert(0);

        for &(start, end) in nums.iter() {
            if !inner.contains(&start) {
                return Err(());
            }

            inner.insert(end);
        }

        Ok(Self { inner })
    }

    pub fn get(&self, range: Range) -> impl Iterator<Item = Range> + '_ {
        let start = self
            .inner
            .range((Included(0), Included(range.start)))
            .last()
            .unwrap();
        let end = self.inner.range(range.end..).next().unwrap();

        self.inner
            .range((Included(start), Included(end)))
            .tuple_windows()
            .map(|(&start, &end)| (start..end))
    }
}
