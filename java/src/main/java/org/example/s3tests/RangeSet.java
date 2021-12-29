package org.example.s3tests;

public class RangeSet {
    public int Start;
    public int End;
    public int Length;

    public RangeSet(int Start, int Length)
    {
        this.Start = Start;
        this.Length = Length;
        End = Start + Length;
    }
    
}
